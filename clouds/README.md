# Cloud stacks

> **Under development.** Only the Databricks stack exists so far (Terraform, notebooks and a helper script). The AWS folder is empty and reserved for later.

Infrastructure as code and run scripts for moving the pipeline off the local Docker Compose stack (`local/`). Each platform has its own folder, its own Terraform state and its own credentials, and is applied on its own.

| Folder | Status | Purpose |
|---|---|---|
| [`databricks/`](databricks/README.md) | In progress | Lakehouse/batch variant on Databricks Free Edition: Terraform for the schema and volumes, notebooks for keyframes, Whisper and Florence, a local script to upload videos. |
| `aws/` | Empty | Reserved for the AWS stacks (persistent and ephemeral). |

## Getting started

1. **Create the env file of the platform you want to work on**, from its template. Env files are gitignored; the `.example` templates are committed.
   ```bash
   cp clouds/databricks/databricks.env.example clouds/databricks/databricks.env
   ```
2. **Fill in the credentials** in that file. Each platform keeps its own file in its own folder (`clouds/<platform>/<platform>.env`), so one platform's secrets are never in another's file.
3. **Load that platform's credentials into your shell** with the helper (bash, must be sourced):
   ```bash
   source clouds/use.sh databricks     # loads clouds/databricks/databricks.env
   source clouds/use.sh --clear        # unloads it
   ```
   Switching platforms unsets the previous platform's variables first, so two sets of credentials are never loaded together. A mistyped name changes nothing.
4. **Follow the platform's README** for the rest: [`databricks/README.md`](databricks/README.md).

## Requirements

- Terraform >= 1.5 (`terraform -version`).
- Network access from the machine running it to the platform's API and to the Terraform registry.
- Python and `pip` for the helper scripts. Each platform lists its own packages in its own `requirements.txt`.

Never commit credentials. `clouds/.gitignore` excludes `*.env` (but keeps `*.env.example`), `*.tfvars`, Terraform state and `.terraform/`.
