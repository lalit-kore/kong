return {
  fields = {
    host = { type = "string", default = "localhost" },
    port = { type = "number", default = 8125 },
    metrics = { type = "array", enum = { "request_count", "latency", "request_size", "status_count" } }, 
    timeout = { type = "number", default = 10000 }
  }
}
