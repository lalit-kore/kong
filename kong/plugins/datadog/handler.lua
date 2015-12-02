local log = require "kong.plugins.datadog.log"
local BasePlugin = require "kong.plugins.base_plugin"
local basic_serializer = require "kong.plugins.log-serializers.basic"

local ngx_now = ngx.now

local DatadogHandler = BasePlugin:extend()

function DatadogHandler:new()
  DatadogHandler.super.new(self, "datadog")
end

function DatadogHandler:body_filter(conf)
  DatadogHandler.super.body_filter(self)
  
  ngx.ctx.datadog = {}
  local eof = ngx.arg[2]
  if eof then -- latest chunk
    ngx.ctx.datadog.response_received = ngx_now() * 1000
  end
end

function DatadogHandler:log(conf)
  DatadogHandler.super.log(self)
  local message = basic_serializer.serialize(ngx)
  message.response.response_received = ngx.ctx.datadog.response_received
  log.execute(conf, message)
end

DatadogHandler.PRIORITY = 1

return DatadogHandler
