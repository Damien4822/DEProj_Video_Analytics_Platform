# Cloud stacks

Infrastructure as code for moving the pipeline off the local Docker Compose stack. Each stack has its own Terraform state and is applied on its own.

| Folder | Purpose |
|---|---|
| `aws/` | AWS stacks: a persistent one (S3, networking, IAM, database) and an ephemeral one (the compute instance), as described in the top-level README. |
| `databricks/` | Databricks workspace objects (catalog, schema, volume, notebooks, jobs) for the lakehouse/batch variant. Targets Free Edition: serverless CPU only, no account-level resources. |

Keep credentials (tokens, access keys) in environment variables, never in `.tf` or `.tfvars` files that get committed.
