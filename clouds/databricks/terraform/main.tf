resource "databricks_schema" "vidpipe" {
  catalog_name = var.catalog_name
  name         = var.schema_name
  comment      = "Video analytics pipeline${var.workspace_name == "" ? "" : " (${var.workspace_name})"}"
}

resource "databricks_volume" "this" {
  for_each = toset(var.volumes)

  catalog_name = databricks_schema.vidpipe.catalog_name
  schema_name  = databricks_schema.vidpipe.name
  name         = each.value
  volume_type  = "MANAGED"
  comment      = "Pipeline volume: ${each.value}"
}
