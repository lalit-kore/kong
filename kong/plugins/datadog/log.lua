local statsd_logger = require "kong.plugins.datadog.statsd_logger"

local ngx_log = ngx.log
local ngx_timer_at = ngx.timer.at

local _M = {}

local METRICS = { "request_count", "latency", "request_size", "status_count" }

local function request_counter(api_name, logger)
  local stat = api_name..".request.count"
  logger:counter(stat, 1, 1)
end

local function status_counter(api_name, message, logger)
  local stat = api_name..".request.status."..message.response.status
  logger:counter(stat, 1, 1)
end

local function request_size_guage(api_name, message, logger)
  local stat = api_name..".request.size"
  logger:gauge(stat, message.request.size, 1)
end

local function latency_guage(api_name, message, logger)
  local latency = message.response.response_received - message.started_at
  local stat = api_name..".latency"
  logger:gauge(stat, latency, 1)
end

local function log(premature, conf, message, logger)

  local logger, err = statsd_logger:new(conf)
  if err then
    ngx_log(ngx.ERR, "failed to create Statsd logger: ", err)
    return
  end
  
  local metrics = conf.metrics
  if not conf.metrics then
    metrics = METRICS
  end

  local api_name = string.gsub(message.api.name, "%.", "_")
  for _, metric in pairs(metrics) do
    if metric == "request_size" then
      request_size_guage(api_name, message, logger)
    end
    if metric == "status_count" then
      status_counter(api_name, message, logger)
    end
    if metric == "latency" then
      latency_guage(api_name, message, logger)
    end
    if metric == "request_count" then
      request_counter(api_name, logger)
    end
  end
 
  logger:close_socket()
end

function _M.execute(conf, message)
  local ok, err = ngx_timer_at(0, log, conf, message)
  if not ok then
    ngx_log(ngx.ERR, "failed to create timer: ", err)
  end
end

return _M
