#!/usr/bin/env lua

local constants = require "kong.constants"
require("lapp")(string.format([[
Restart the Kong instance running in the configured 'nginx_working_dir'.

Kong will be shutdown before restarting. For a zero-downtime reload
of your configuration, look at 'kong reload'.

Usage: kong restart [options]

Options:
  -c,--config (default %s) path to configuration file
]], constants.CLI.GLOBAL_KONG_CONF))

require("kong.cli.cmds.stop")
require("kong.cli.cmds.start")