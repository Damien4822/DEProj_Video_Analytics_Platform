# Databricks stack

Terraform for the Databricks workspace objects of the lakehouse/batch variant of the pipeline. It targets **Databricks Free Edition**: serverless compute only (no custom clusters, no GPU), one workspace, no account-level API, and outbound internet limited to a short list of domains.

Terraform only declares objects (schema, volumes, later notebooks and a job). The processing code runs in notebooks on Databricks compute.

## What it creates

| Resource | Purpose |
|---|---|
| Schema `vidpipe` (in your catalog) | Holds the pipeline's tables and volumes |
| Volumes `raw`, `models`, `frames` | Videos, model weights, keyframe images |

Files in `terraform/`: `versions.tf` (provider pin), `provider.tf` (empty block, auth comes from the environment), `variables.tf`, `main.tf`, `outputs.tf`.

## Requirements

- A Databricks Free Edition workspace, with a **catalog** already created (Terraform creates the schema inside it, not the catalog).
- A **personal access token**: user menu > Settings > Developer > Access tokens > Generate new token. Use a short lifetime and copy it once.
- **Terraform >= 1.5** installed (`terraform -version`).
- Network access from the machine running Terraform to `https://<workspace>.cloud.databricks.com`, and to the Terraform registry for `terraform init`.

## Setup

```bash
cd clouds/databricks
cp databricks.env.example databricks.env     # gitignored; fill in the values
```

| Variable | Required | Meaning |
|---|---|---|
| `DATABRICKS_HOST` | yes | Workspace URL, `https://dbc-xxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | yes | The personal access token |
| `TF_VAR_catalog_name` | yes | Name of your existing catalog (see the Catalog view) |
| `TF_VAR_schema_name` | no | Defaults to `vidpipe` |
| `TF_VAR_workspace_name` | no | Label used only in the schema comment |

Never commit `databricks.env`, tokens or `*.tfvars`. `clouds/.gitignore` covers them.

## Step 1: test the connection

Do this before running Terraform. It separates network and token problems from Terraform problems.

```bash
cd clouds/databricks
set -a; source databricks.env; set +a        # `set -a` exports the variables to child processes

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

Run in the same shell as step 1, so the variables are loaded.

```bash
cd terraform
terraform init       # downloads the databricks provider
terraform plan       # shows what would be created; changes nothing
terraform apply      # creates the schema and volumes
terraform output     # prints the /Volumes/... paths for notebooks
```

If `plan` reports an authentication error that points to the OAuth page, the provider found no usable token. Check, in order:
1. The variables are exported in this shell (`echo "${DATABRICKS_TOKEN:+set}"`), not only sourced.
2. Step 1 returned `200` for the token.
3. No `DATABRICKS_CONFIG_PROFILE`, `DATABRICKS_AUTH_TYPE` or `~/.databrickscfg` is overriding the token.
4. The env file has no quotes, trailing spaces or Windows line endings.

To remove everything Terraform created: `terraform destroy`.

## Free Edition notes

- Terraform support on Free Edition has not been confirmed in the official docs. This stack is the test: if `plan` or `apply` fails on a specific resource, note which one here.
- Terraform runs on your machine and only calls the workspace API. Databricks Asset Bundles run Terraform on Databricks itself, and reports suggest the outbound domain restriction blocks that.
- Account-level resources (creating workspaces, users, metastores) are not available.

## Next

1. Upload a short H.264 mp4 and the model weights to the volumes by hand.
2. Notebooks for keyframe extraction, Whisper and Florence, and chunking.
3. A serverless job that runs them in order, defined in `jobs.tf`.
