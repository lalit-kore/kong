#!/usr/bin/env lua

local constants = require "kong.constants"
local configuration = require "kong.cli.utils.configuration"
local logger = require "kong.cli.utils.logger"
local services = require "kong.cli.utils.services"

local args = require("lapp")(string.format([[
Start Kong with given configuration. Kong will run in the configured 'nginx_working_dir' directory.

Usage: kong start [options]

Options:
  -c,--config (default %s) path to configuration file
]], constants.CLI.GLOBAL_KONG_CONF))

logger:info("Kong "..constants.VERSION)

local config, err = configuration.parse(args.config)
if err then
  logger:error(err)
  os.exit(1)
end

local ok, err = services.start_all(config)
if ok then
  logger:success("Started")
else
  services.stop_all(config)
  logger:error(err)
  os.exit(1)
end