#!/usr/bin/env lua

local constants = require "kong.constants"
local logger = require "kong.cli.utils.logger"
local utils = require "kong.tools.utils"
local configuration = require "kong.cli.utils.configuration"
local serf = require "kong.cli.services.serf"
local args = require("lapp")(string.format([[
Kong cluster operations.

Usage: kong cluster <command> <args> [options]

Commands:
  <command> (string) where <command> is one of:
                       join, members, reachability, keygen

Options:
  -c,--config (default %s) path to configuration file

]], constants.CLI.GLOBAL_KONG_CONF))

local JOIN = "join"
local SUPPORTED_COMMANDS = { JOIN, "members", "keygen", "reachability" }

if not utils.table_contains(SUPPORTED_COMMANDS, args.command) then
  logger:error("Invalid cluster command. Supported commands are: "..table.concat(SUPPORTED_COMMANDS, ", "))
  os.exit(1)
end

local config, err = configuration.parse(args.config)
if err then
  logger:error(err)
  os.exit(1)
end

local signal = args.command
args.command = nil
args.config = nil

if signal == JOIN then
  if utils.table_size(args) ~= 1 then
    logger:error("You must specify one address")
    os.exit(1)
  end
end

local res, err = serf(config.value):invoke_signal(signal, args)
if err then
  logger:error(err)
else
  logger:print(res)
end