return {
  name = "Node",
  primary_key = {"name"},
  fields = {
    name = { type = "string" },
    created_at = { type = "timestamp", dao_insert_value = true },
    address = { type = "string", queryable = true, required = true }
  }
}
