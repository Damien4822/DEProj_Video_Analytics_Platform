output "schema_full_name" {
  value = "${databricks_schema.vidpipe.catalog_name}.${databricks_schema.vidpipe.name}"
}

output "volume_paths" {
  description = "Paths to use from notebooks and jobs."
  value = {
    for name, v in databricks_volume.this :
    name => "/Volumes/${v.catalog_name}/${v.schema_name}/${v.name}"
  }
}
