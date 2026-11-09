"""Download a video with yt-dlp and upload it to the Databricks `raw` volume, then delete the local copy.

Why locally: Databricks compute IPs are often challenged by YouTube, a home or office IP usually is not.
Credentials and names come from databricks.env (DATABRICKS_HOST, DATABRICKS_TOKEN, TF_VAR_catalog_name,
TF_VAR_schema_name), the same file Terraform uses.

    source clouds/use.sh databricks        # from the repo root; loads clouds/databricks/databricks.env
    python clouds/databricks/scripts/upload_video.py "<video url>"

The volume keeps a small register of submitted videos, `raw/_sources.json` (URL -> file name). Before anything is
downloaded the script looks the URL up there, without contacting the video site at all; if the video is registered
and its file is still in the volume, it stops and prints the file name. Videos missing from the register are
recognised by their id (a metadata-only yt-dlp call) and added to it. --force skips both checks.

If DATABRICKS_WAREHOUSE_ID is set, it also asks the `videos` table (SQL warehouse) which source URLs were already
processed, so a video whose file was later deleted from `raw` is not downloaded again. Without it that check is skipped.

    python scripts/upload_video.py --list      # show the videos already in the volume, no URL needed

Needs: yt-dlp[default], ffmpeg on the PATH (to merge video and audio), and the repo's python-dotenv.
The local file is deleted only after the upload is verified (remote size equals local size).
"""
import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTER = "_sources.json"   # file name of the register inside the volume; notebooks ignore it (not a video)


def source_key(url):
    """Stable key for a URL, computed offline: 'Youtube:<id>' for known sites (so youtu.be and watch?v= links
    to one video match), otherwise the URL itself without its fragment."""
    url = url.strip().split("#")[0]
    try:
        from yt_dlp.extractor import gen_extractors

        for ie in gen_extractors():
            if ie.ie_key() != "Generic" and ie.suitable(url):
                return f"{ie.ie_key()}:{ie._match_id(url)}"
    except Exception:
        pass
    return url


def headers_for(token):
    return {"Authorization": f"Bearer {token}"}


def read_register(host, token, directory):
    response = requests.get(files_api_url(host, f"{directory}/{REGISTER}"), headers=headers_for(token), timeout=60)
    if response.status_code == 404:
        return {}
    response.raise_for_status()
    return response.json()


def write_register(host, token, directory, register):
    response = requests.put(files_api_url(host, f"{directory}/{REGISTER}"), params={"overwrite": "true"},
                            data=json.dumps(register, indent=2).encode(), headers=headers_for(token), timeout=60)
    response.raise_for_status()


class SqlError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(f"{code}: {message}")
        self.code = code


def run_sql(host, token, warehouse_id, statement, timeout_s=300):
    """Run one statement on a SQL warehouse (Statement Execution API) and return its rows."""
    base = f"{host.rstrip('/')}/api/2.0/sql/statements"
    body = {"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "30s", "on_wait_timeout": "CONTINUE"}
    response = requests.post(base, json=body, headers=headers_for(token), timeout=90)
    response.raise_for_status()
    result = response.json()
    deadline = time.time() + timeout_s   # a stopped warehouse needs a while to start
    while result["status"]["state"] in ("PENDING", "RUNNING"):
        if time.time() > deadline:
            raise TimeoutError("the SQL warehouse did not answer in time")
        time.sleep(3)
        response = requests.get(f"{base}/{result['statement_id']}", headers=headers_for(token), timeout=60)
        response.raise_for_status()
        result = response.json()
    if result["status"]["state"] != "SUCCEEDED":
        error = result["status"].get("error", {})
        raise SqlError(error.get("error_code", "UNKNOWN"), error.get("message", ""))
    return (result.get("result") or {}).get("data_array") or []


def processed_sources(host, token, warehouse_id, catalog, schema):
    """{source key: file name} for every video in the `videos` table that has a source URL.
    A missing table, or a table without the source_url column yet, counts as empty."""
    try:
        rows = run_sql(host, token, warehouse_id,
                       f"SELECT source_url, filename FROM {catalog}.{schema}.videos WHERE source_url IS NOT NULL")
    except SqlError as exc:
        if exc.code in ("TABLE_OR_VIEW_NOT_FOUND", "UNRESOLVED_COLUMN") or "source_url" in str(exc):
            return {}
        raise
    return {source_key(url): filename for url, filename in rows}


def register_entry(register, key, url, filename, title):
    register[key] = {"url": url, "filename": filename, "title": title,
                     "registered_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}


def ytdlp_options(workdir):
    """Same format selector as the local downloader (no AV1, up to 720p)."""
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from vidpipe.config import cfg

    options = {
        "format": os.environ.get("YTDLP_FORMAT") or cfg.ytdlp_format,
        "outtmpl": str(Path(workdir) / "%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
    }
    if cfg.ytdlp_no_certifi:
        options["compat_opts"] = {"no-certifi"}
    return options


def video_info(url, workdir):
    """Metadata only (no download): confirms the URL works and gives the video id and title."""
    import yt_dlp

    with yt_dlp.YoutubeDL(ytdlp_options(workdir)) as ydl:
        info = ydl.extract_info(url, download=False)
    return info["id"], info.get("title")


def list_files(host, token, directory):
    """(name, size in bytes) of every file in the volume directory; empty if it does not exist."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{host.rstrip('/')}/api/2.0/fs/directories{quote(directory)}"
    names, page_token = [], None
    while True:
        params = {"page_token": page_token} if page_token else {}
        response = requests.get(url, headers=headers, params=params, timeout=60)
        if response.status_code == 404:
            return []   # directory does not exist yet
        response.raise_for_status()
        body = response.json()
        names += [(c["name"], c.get("file_size")) for c in body.get("contents", [])
                  if not c.get("is_directory") and c["name"] != REGISTER]
        page_token = body.get("next_page_token")
        if not page_token:
            break
    return names


def download(url, workdir):
    import yt_dlp

    with yt_dlp.YoutubeDL(ytdlp_options(workdir)) as ydl:
        info = ydl.extract_info(url, download=True)
        path = (info.get("requested_downloads") or [{}])[0].get("filepath") or ydl.prepare_filename(info)
    return Path(path), info.get("title")


def files_api_url(host, volume_path):
    return f"{host.rstrip('/')}/api/2.0/fs/files{quote(volume_path)}"


def upload(path, host, token, volume_path):
    """PUT the file into the volume (overwriting), then check the remote size matches."""
    headers = {"Authorization": f"Bearer {token}"}
    url = files_api_url(host, volume_path)
    size = path.stat().st_size
    with open(path, "rb") as f:   # streamed, not read into memory
        response = requests.put(url, params={"overwrite": "true"}, data=f, headers=headers, timeout=3600)
    response.raise_for_status()
    head = requests.head(url, headers=headers, timeout=60)
    head.raise_for_status()
    remote = int(head.headers["Content-Length"])
    if remote != size:
        raise RuntimeError(f"upload verification failed: local {size} bytes, remote {remote} bytes")
    return size


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("url", nargs="?", help="video URL (not needed with --list)")
    parser.add_argument("--list", action="store_true", help="show the videos already in the volume and exit")
    parser.add_argument("--catalog", default=os.environ.get("TF_VAR_catalog_name"))
    parser.add_argument("--schema", default=os.environ.get("TF_VAR_schema_name", "vidpipe"))
    parser.add_argument("--volume", default="raw")
    parser.add_argument("--warehouse", default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
                        help="SQL warehouse id, to check the videos table (default: DATABRICKS_WAREHOUSE_ID)")
    parser.add_argument("--force", action="store_true", help="download and upload even if the video is already there")
    args = parser.parse_args()

    host, token = os.environ.get("DATABRICKS_HOST"), os.environ.get("DATABRICKS_TOKEN")
    if not (host and token and args.catalog):
        sys.exit("set DATABRICKS_HOST, DATABRICKS_TOKEN and TF_VAR_catalog_name (source databricks.env first)")

    directory = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"
    if args.list:
        files = list_files(host, token, directory)
        urls = {e["filename"]: e["url"] for e in read_register(host, token, directory).values()}
        print(f"{len(files)} file(s) in {directory}")
        for name, size in sorted(files):
            print(f"  {name}  ({(size or 0) / 1e6:.1f} MB)  {urls.get(name, '(no URL registered)')}")
        return
    if not args.url:
        sys.exit("give a video URL, or use --list to see what is already uploaded")

    key = source_key(args.url)
    register = read_register(host, token, directory)
    present = {name for name, _ in list_files(host, token, directory)}
    if not args.force:
        entry = register.get(key)
        if entry and entry["filename"] in present:
            print(f"already submitted: {entry['title']!r} is in {directory} as {entry['filename']}; "
                  f"nothing downloaded (--force to redo).\nUse filename = {entry['filename']} in notebooks 01 and 02.")
            return

    if not args.force and args.warehouse:
        done = processed_sources(host, token, args.warehouse, args.catalog, args.schema).get(key)
        if done:
            where = "and its file is in raw" if done in present else "but its file is no longer in raw"
            print(f"already processed: the videos table has this URL as {done} ({where}); nothing downloaded "
                  f"(--force to redo).\nUse filename = {done} in notebooks 01 and 02.")
            return
    elif not args.warehouse:
        print("note: DATABRICKS_WAREHOUSE_ID is not set, so the videos table was not checked", file=sys.stderr)

    workdir = tempfile.mkdtemp(prefix="upload_video_")
    try:
        video_id, title = video_info(args.url, workdir)
        found = [n for n in present if Path(n).stem == video_id]
        if found and not args.force:
            shutil.rmtree(workdir, ignore_errors=True)
            register_entry(register, key, args.url, found[0], title)   # file predates the register: add it
            write_register(host, token, directory, register)
            print(f"{title!r} is already in {directory} as {found[0]} (now registered); nothing downloaded "
                  f"(--force to redo).\nUse filename = {found[0]} in notebooks 01 and 02.")
            return
        path, title = download(args.url, workdir)
        volume_path = f"{directory}/{path.name}"
        print(f"downloaded {title!r} ({path.stat().st_size / 1e6:.1f} MB), uploading to {volume_path}")
        upload(path, host, token, volume_path)
        register_entry(register, key, args.url, path.name, title)
        write_register(host, token, directory, register)
    except BaseException:
        print(f"failed; the local files are kept in {workdir}", file=sys.stderr)
        raise
    shutil.rmtree(workdir)
    print(f"uploaded and verified; local copy removed.\nIn notebooks 01 and 02 use filename = {path.name}")


if __name__ == "__main__":
    main()
