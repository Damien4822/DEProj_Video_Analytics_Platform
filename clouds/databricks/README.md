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
│   └── 02_transcribe_and_describe.py  Whisper + Florence-2 + OCR -> audio, visual, object and OCR tables
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
| Volumes `raw`, `models`, `frames` | Videos, model weights and the Hugging Face cache, keyframe images |

The notebooks create the Delta tables themselves: `videos`, `frames`, `audio_segments`, `visual_segments`, `detected_objects`, `ocr_items`.

## Requirements

- A Databricks Free Edition workspace with a **catalog** already created (Terraform creates the schema inside it, not the catalog).
- A **personal access token**: user menu > Settings > Developer > Access tokens > Generate new token. Use a short lifetime and copy it once.
- **Terraform >= 1.5** (`terraform -version`).
- Network access from your machine to `https://<workspace>.cloud.databricks.com` and to the Terraform registry.
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
| `No route to host` or timeout | The network blocks the workspace. Run from another machine or ask for `*.cloud.databricks.com` to be allowed. |
| Certificate error | A proxy re-signs HTTPS. Point `SSL_CERT_FILE` at your CA bundle. |
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

Either upload an mp4 by hand in the Catalog view, or use the helper, which downloads on your machine and uploads through the Files API (the local copy is deleted once the remote size matches):

```bash
pip install -r clouds/databricks/requirements.txt
python clouds/databricks/scripts/upload_video.py "<video url>"      # after `source clouds/use.sh databricks`
```

## Step 4: run the notebooks

1. Create a **Git folder** in the workspace that clones this repo, so notebooks can import `src/vidpipe`. After each push, press Pull.
2. Open `notebooks/00_smoke_test`, attach serverless compute, fill in the `catalog` widget and run it.
3. Run `01_extract_keyframes`, then `02_transcribe_and_describe` (attach serverless **GPU** for 02, or set its `device` widget to `cpu`).
4. Each notebook installs its own pinned packages in its first `%pip` cell. If a version error appears, fix only the versions and copy the working pins back into the repo. Notebook 02 prints the versions that worked in its last cell.

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
