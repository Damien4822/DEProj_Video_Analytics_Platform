# Cloud stacks

> **Under development.** Only the Databricks stack exists so far (Terraform, notebooks and a helper script). The AWS folder is empty and reserved for later.

Infrastructure as code and run scripts for moving the pipeline off the local Docker Compose stack (`local/`). Each platform has its own folder, its own Terraform state and its own credentials, and is applied on its own.

| Folder | Status | Purpose |
|---|---|---|
| [`databricks/`](databricks/README.md) | In progress | Lakehouse/batch variant on Databricks Free Edition: Terraform for the schema and volumes, notebooks for keyframes, Whisper and Florence, a local script to upload videos. |
| `aws/` | Empty | Reserved for the AWS stacks (persistent and ephemeral). |

## How credentials work

One entry point for every platform: **`source clouds/use.sh <platform>`**, run from the repo root.

- Each platform keeps its credentials in its own folder, `clouds/<platform>/<platform>.env`, created from the committed `<platform>.env.example` next to it. These files are gitignored.
- `use.sh` loads the chosen file into your current shell. Terraform and the helper scripts read the credentials from the environment, so nothing is passed on the command line.
- Switching platforms unsets the previous platform's variables first, so two sets of credentials are never loaded together. A mistyped name changes nothing.

```bash
cp clouds/databricks/databricks.env.example clouds/databricks/databricks.env   # once, then fill it in
source clouds/use.sh databricks                                                 # in every new shell
source clouds/use.sh --clear                                                    # unload
```

`use.sh` is for bash and must be sourced (not executed), in the same terminal where you then run Terraform or a script.

## Getting started

1. **Create and fill the platform's env file** (above). You need the platform's account first: for Databricks, a Free Edition workspace, a catalog and an access token.
2. **Load it** with `source clouds/use.sh <platform>`.
3. **Continue in the platform's README.** The steps below are the same on every platform; the README has the commands.

| Step | Purpose | Where |
|---|---|---|
| Test the connection | Separates network and token problems from Terraform problems before anything is created. | Platform README, "Step 1" |
| Terraform plan and apply | Creates the workspace objects the pipeline needs (schema, storage volumes) and can remove them again with `destroy`. | Platform README, "Step 2" |
| Get a video into storage | Puts the input in the cloud storage. Downloading happens on your machine because cloud IPs are often blocked by video sites. | Platform README, "Step 3" |
| Run the pipeline | Runs the processing code (keyframes, transcription, descriptions) on the platform's compute. | Platform README, "Step 4" |

Start here: **[`databricks/README.md`](databricks/README.md)**. It also has the folder layout and what each folder is for.

## Requirements

- Terraform >= 1.5 (`terraform -version`).
- Internet access from the machine running it, to the platform's API and to the Terraform registry.
- Python and `pip` for the helper scripts. Each platform lists its own packages in its own `requirements.txt`.

Never commit credentials. `clouds/.gitignore` excludes `*.env` (but keeps `*.env.example`), `*.tfvars`, Terraform state and `.terraform/`.
