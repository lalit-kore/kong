#!/usr/bin/env lua

local constants = require "kong.constants"
local logger = require "kong.cli.utils.logger"
local configuration = require "kong.cli.utils.configuration"
local args = require("lapp")(string.format([[
Gracefully reload the Kong instance running in the configured 'nginx_working_dir'.

Any configuration change will be applied.

Usage: kong reload [options]

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

local _, err = nginx:reload()
if err then
  logger:error(err)
  os.exit(1)
else
  logger:success("Reloaded")
end