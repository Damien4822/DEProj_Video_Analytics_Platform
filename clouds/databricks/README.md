# Databricks stack

Terraform, notebooks and a helper script for the Databricks (lakehouse/batch) variant of the pipeline. It targets **Databricks Free Edition**: serverless compute only (no custom clusters; the serverless GPU option on this account is a single A10), one workspace, no account-level API, and outbound internet limited to a list of trusted domains.

Division of labour: **Terraform declares the objects** (schema, volumes; later a job), **notebooks hold the processing code** and run on Databricks compute, and **local scripts** do what must happen on your own machine.

## Folder layout

```
clouds/databricks/
├── README.md                  this file
├── databricks.env.example     template for the credentials (copy to databricks.env, gitignored)
├── requirements.txt           Python packages for the scripts in scripts/ (not for the notebooks)
├── terraform/                 infrastructure as code, run from your machine
│   ├── versions.tf            Terraform and provider version pins
│   ├── provider.tf            empty provider block; host and token come from the environment
│   ├── variables.tf           inputs: catalog_name (required), schema_name, workspace_name, volumes
│   ├── main.tf                the schema and the volumes
│   └── outputs.tf             the schema name and the /Volumes/... paths for notebooks
├── notebooks/                 processing code, run on Databricks (pulled in through a Git folder)
│   ├── 00_smoke_test.py       checks what the compute allows: packages, CPU/RAM, repo import, network
│   ├── 01_extract_keyframes.py  keyframes from the videos in `raw` -> `videos` and `frames` tables
│   └── 02_transcribe_and_describe.py  Whisper + Florence-2 + OCR -> audio, visual, object and OCR tables (resumable)
└── scripts/                   helpers that run on your own machine
    └── upload_video.py        download a video with yt-dlp, upload it to the `raw` volume, delete the local copy
```

| Folder | What lives there | Where it runs |
|---|---|---|
| `terraform/` | Declarations of what must exist in the workspace. Creating, changing and destroying objects. Never processes data. | Your machine, calling the workspace API |
| `notebooks/` | The pipeline stages, reusing `src/vidpipe` (keyframes, quality checks, `FrameDescriber`). Written as plain `.py` files in Databricks source format so they diff in git. | Databricks serverless compute |
| `scripts/` | Work that cannot run on Databricks, such as downloading from YouTube (cloud IPs are often challenged). | Your machine |

## What Terraform creates

| Resource | Purpose |
|---|---|
| Schema `vidpipe` (in your catalog) | Holds the pipeline's tables and volumes |
| Volumes `raw`, `models`, `frames` | Videos plus the `_sources.json` register of the URLs they came from (written by the upload script), model weights and the Hugging Face cache, keyframe images |

The notebooks create the Delta tables themselves (`videos` includes the `source_url` the video came from): `videos`, `frames`, `audio_segments`, `visual_segments`, `detected_objects`, `ocr_items`.

## Requirements

- A Databricks Free Edition workspace with a **catalog** already created (Terraform creates the schema inside it, not the catalog).
- A **personal access token**: user menu > Settings > Developer > Access tokens > Generate new token. Use a short lifetime and copy it once.
- **Terraform >= 1.5** (`terraform -version`).
- Internet access from your machine to `https://<workspace>.cloud.databricks.com` and to the Terraform registry.
- For the scripts: `pip install -r requirements.txt`, and `ffmpeg` on the PATH.

## Setup

```bash
cp clouds/databricks/databricks.env.example clouds/databricks/databricks.env    # then fill it in
source clouds/use.sh databricks                                                  # from the repo root
```

| Variable | Required | Meaning |
|---|---|---|
| `DATABRICKS_HOST` | yes | Workspace URL, `https://dbc-xxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | yes | The personal access token |
| `DATABRICKS_WAREHOUSE_ID` | no | Id of a SQL warehouse (the last part of its HTTP path under SQL Warehouses > Connection details). Lets the upload script check the `videos` table for already-processed URLs; without it that check is skipped |
| `TF_VAR_catalog_name` | yes | Name of your existing catalog (see the Catalog view) |
| `TF_VAR_schema_name` | no | Defaults to `vidpipe` |
| `TF_VAR_workspace_name` | no | Label used only in the schema comment |

Never commit `databricks.env`, tokens or `*.tfvars`.

## Step 1: test the connection

Do this before Terraform. It separates network and token problems from Terraform problems. Run it in the shell where you sourced the credentials.

```bash
# Is the workspace reachable? Any HTTP response (a 404 is normal) means yes.
curl -sSI -m 15 "$DATABRICKS_HOST" | head -1

# Does the token work? Expect 200; 401 or 403 means a bad or expired token.
curl -s -o /dev/null -w "API status: %{http_code}\n" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "${DATABRICKS_HOST%/}/api/2.0/preview/scim/v2/Me"
```

| Result | Meaning and fix |
|---|---|
| `No route to host` or timeout | The workspace is not reachable from this machine. Check `DATABRICKS_HOST` and your internet connection. |
| `API status: 401` or `403` | Token is invalid or expired. Generate a new one. |
| `API status: 200` | Connection and token are fine. Continue. |

## Step 2: plan and apply

```bash
cd clouds/databricks/terraform
terraform init       # downloads the databricks provider
terraform plan       # shows what would be created; changes nothing
terraform apply      # creates the schema and volumes
terraform output     # prints the /Volumes/... paths for notebooks
```

If `plan` reports an authentication error that points to the OAuth page, the provider found no usable token. Check, in order:
1. The credentials are loaded in this shell (`echo "${DATABRICKS_TOKEN:+set}"`), using `source clouds/use.sh databricks`, in the same terminal.
2. Step 1 returned `200` for the token.
3. No `DATABRICKS_CONFIG_PROFILE`, `DATABRICKS_AUTH_TYPE` or `~/.databrickscfg` is overriding the token.
4. The env file has no quotes, trailing spaces or Windows line endings.

`terraform destroy` removes everything Terraform created, including the volumes and the files in them. Terraform state is local (gitignored), so run it from the machine where you applied.

## Step 3: get a video into the `raw` volume

**Optional first: see what is already in `raw`**, so you can reuse a video (and its file name) instead of uploading it again:

```bash
pip install -r clouds/databricks/requirements.txt
python clouds/databricks/scripts/upload_video.py --list      # after `source clouds/use.sh databricks`
```

It prints each file name, its size and the URL it was submitted from. The name is what you later paste into the `filename` widget of notebooks 01 and 02.

**To add a new video:** upload a video (mp4, webm, mkv or mov) by hand in the Catalog view, or use the helper, which downloads on your machine and uploads through the Files API (the local copy is deleted once the remote size matches):

```bash
python clouds/databricks/scripts/upload_video.py "<video url>"      # after `source clouds/use.sh databricks`
```

**How duplicates are avoided.** The `raw` volume holds a small register, `_sources.json`, mapping each submitted URL to its file name (YouTube links are matched by video id, so `youtu.be/...` and `watch?v=...` count as the same video). The helper checks it first, **before contacting the video site at all**. If the URL is registered and its file is still in `raw`, it stops and prints the file name. A video that is in `raw` but not in the register (uploaded by hand or earlier) is recognised by its id and added to the register. After a new upload the URL and file name are recorded. Add `--force` to skip the checks and upload again. When it finishes it prints the `filename` to paste into notebooks 01 and 02. The notebooks ignore `_sources.json`, except that 01 copies the URL from it into the `source_url` column of the `videos` table.

**Checking the table too.** If `DATABRICKS_WAREHOUSE_ID` is set, the helper also asks the `videos` table which URLs were already processed. That catches a video whose file was later deleted from `raw`: the table still knows it was processed, so it is not downloaded again. This uses the SQL warehouse, which may take a minute to start. Without the id the helper prints a note and checks only `raw`. Videos processed before this change have no `source_url` until notebook 01 is re-run on them.

## Step 4: run the notebooks

1. Create a **Git folder** in the workspace that clones this repo, so notebooks can import `src/vidpipe`. After each push, press Pull.
2. Open `notebooks/00_smoke_test`, attach serverless compute, fill in the `catalog` widget and run it.
3. Have the video in `raw` first (Step 3). Run `01_extract_keyframes`, then `02_transcribe_and_describe` (attach serverless **GPU** for 02, or set its `device` widget to `cpu`).
4. **One video at a time:** both notebooks require the `filename` widget (the video's name in `raw`, e.g. `abc123.webm`) and stop if it is empty, so nothing else in `raw` is processed. Notebook 02 is resumable: it skips audio that is already transcribed and frames that are already described, and saves vision results every `write_every` frames, so a quota cut loses at most one batch. After re-running 01 on a video, set 02's `force` widget to `yes` so it redoes that video.
5. Each notebook installs its own pinned packages in its first `%pip` cell. If a version error appears, fix only the versions and copy the working pins back into the repo. Notebook 02 prints the versions that worked in its last cell.

The CPU compute seen here had 4 CPUs and about 6 GB of free RAM. Notebook 02 prints seconds of compute per minute of video, for the scaling notes.

## Free Edition notes

- Terraform works on Free Edition from your own machine (apply and destroy tested). Databricks Asset Bundles run Terraform on Databricks itself, and reports suggest the outbound domain restriction blocks that; not tested here.
- Account-level resources (creating workspaces, users, metastores) are not available.
- Compute and GPU use is quota-limited. When a quota is exceeded the compute goes offline until it resets; data and settings stay.
- Databricks compute IPs may be challenged by YouTube. Use `scripts/upload_video.py` instead of downloading in a notebook.

## Next

1. Notebook `03` to build the chunks, reusing `vidpipe.chunking`.
2. The embedder (bge-m3 fits on the A10) and a Delta table for the vectors.
3. `jobs.tf`: a serverless job that runs the notebooks in order.
