from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for c in w.clusters.list():
  print(c.cluster_name)