"""Download a video with yt-dlp and upload it to the Databricks `raw` volume, then delete the local copy.

Why locally: Databricks compute IPs are often challenged by YouTube, a home or office IP usually is not.
Credentials and names come from databricks.env (DATABRICKS_HOST, DATABRICKS_TOKEN, TF_VAR_catalog_name,
TF_VAR_schema_name), the same file Terraform uses.

    cd clouds/databricks
    set -a; source databricks.env; set +a
    python scripts/upload_video.py "<video url>"

Needs: yt-dlp[default], ffmpeg on the PATH (to merge video and audio), and the repo's python-dotenv.
The local file is deleted only after the upload is verified (remote size equals local size).
"""
import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.parse import quote

import requests

REPO_ROOT = Path(__file__).resolve().parents[3]


def download(url, workdir):
    """Download with the same format selector as the local downloader (no AV1, up to 720p)."""
    import yt_dlp

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
    with yt_dlp.YoutubeDL(options) as ydl:
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
    parser.add_argument("url", help="video URL")
    parser.add_argument("--catalog", default=os.environ.get("TF_VAR_catalog_name"))
    parser.add_argument("--schema", default=os.environ.get("TF_VAR_schema_name", "vidpipe"))
    parser.add_argument("--volume", default="raw")
    args = parser.parse_args()

    host, token = os.environ.get("DATABRICKS_HOST"), os.environ.get("DATABRICKS_TOKEN")
    if not (host and token and args.catalog):
        sys.exit("set DATABRICKS_HOST, DATABRICKS_TOKEN and TF_VAR_catalog_name (source databricks.env first)")

    workdir = tempfile.mkdtemp(prefix="upload_video_")
    try:
        path, title = download(args.url, workdir)
        volume_path = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}/{path.name}"
        print(f"downloaded {title!r} ({path.stat().st_size / 1e6:.1f} MB), uploading to {volume_path}")
        upload(path, host, token, volume_path)
    except BaseException:
        print(f"failed; the local files are kept in {workdir}", file=sys.stderr)
        raise
    shutil.rmtree(workdir)
    print(f"uploaded and verified; local copy removed.\nIn notebook 01 the video is now in {volume_path}")


if __name__ == "__main__":
    main()
