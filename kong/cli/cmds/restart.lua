#!/usr/bin/env lua

local constants = require "kong.constants"
local configuration = require "kong.cli.utils.configuration"
local logger = require "kong.cli.utils.logger"
local services = require "kong.cli.utils.services"

local args = require("lapp")(string.format([[
Restart the Kong instance running in the configured 'nginx_working_dir'.

Kong will be shutdown before restarting. For a zero-downtime reload
of your configuration, look at 'kong reload'.

Usage: kong restart [options]

Options:
  -c,--config (default %s) path to configuration file
]], constants.CLI.GLOBAL_KONG_CONF))

local config, err = configuration.parse(args.config)
if err then
  logger:error(err)
  os.exit(1)
end

services.stop_all(config)

require("kong.cli.cmds.start")