#!/usr/bin/env lua

local constants = require "kong.constants"
local logger = require "kong.cli.utils.logger"
local services = require "kong.cli.utils.services"
local configuration = require "kong.cli.utils.configuration"
local args = require("lapp")(string.format([[
Checks the status of Kong and its services. Returns an error if the services are not properly running.

Usage: kong status [options]

Options:
  -c,--config (default %s) path to configuration file
]], constants.CLI.GLOBAL_KONG_CONF))

local config, err = configuration.parse(args.config)
if err then
  logger:error(err)
  os.exit(1)
end

local status = services.check_status(config)
if status == services.STATUSES.ALL_RUNNING then
  logger:info("Kong is running")
  os.exit(0)
elseif status == services.STATUSES.SOME_RUNNING then
  logger:error("Some services required by Kong are not running. Please execute \"kong restart\"!")
  os.exit(1)
else
  logger:error("Kong is not running")
  os.exit(1)
end