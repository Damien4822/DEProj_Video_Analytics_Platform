variable "workspace_name" {
  description = "Informational only; used in descriptions. The provider identifies the workspace by DATABRICKS_HOST."
  type        = string
  default     = ""
}

variable "catalog_name" {
  description = "Existing Unity Catalog catalog to create the schema in (see the Catalog view in the workspace)."
  type        = string
}

variable "schema_name" {
  description = "Schema that holds the pipeline's tables and volumes."
  type        = string
  default     = "vidpipe"
}

variable "volumes" {
  description = "Volumes to create in the schema: raw videos, model weights, keyframe images."
  type        = list(string)
  default     = ["raw", "models", "frames"]
}
