#!/usr/bin/env lua

local constants = require "kong.constants"
local logger = require "kong.cli.utils.logger"
local services = require "kong.cli.utils.services"
local configuration = require "kong.cli.utils.configuration"
local args = require("lapp")(string.format([[
Fast shutdown. Stop the Kong instance running in the configured 'nginx_working_dir' directory.

Usage: kong stop [options]

Options:
  -c,--config (default %s) path to configuration file
]], constants.CLI.GLOBAL_KONG_CONF))

local config, err = configuration.parse(args.config)
if err then
  logger:error(err)
  os.exit(1)
end

services.stop_all(config)

logger:success("Stopped")