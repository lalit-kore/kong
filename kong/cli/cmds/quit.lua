#!/usr/bin/env lua

local constants = require "kong.constants"
local logger = require "kong.cli.utils.logger"
local configuration = require "kong.cli.utils.configuration"
local services = require "kong.cli.utils.services"
local args = require("lapp")(string.format([[
Graceful shutdown. Stop the Kong instance running in the configured 'nginx_working_dir' directory.

Usage: kong stop [options]

Options:
  -c,--config (default %s) path to configuration file
]], constants.CLI.GLOBAL_KONG_CONF))

local config, err = configuration.parse(args.config)
if err then
  logger:error(err)
  os.exit(1)
end

local nginx = require("kong.cli.services.nginx")(config.value, config.path)

if not nginx:is_running() then
  logger:error("Kong is not running")
  os.exit(1)
end

nginx:quit()
while(nginx:is_running()) do
  -- Wait until it quits
end

services.stop_all(config)
logger:success("Stopped")