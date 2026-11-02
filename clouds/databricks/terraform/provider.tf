# Empty on purpose: the provider reads DATABRICKS_HOST and DATABRICKS_TOKEN from the environment
# (see ../databricks.env.example), so no credential is ever written in a .tf file.
provider "databricks" {}
