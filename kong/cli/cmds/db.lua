#!/usr/bin/env lua

local Faker = require "kong.tools.faker"
local constants = require "kong.constants"
local logger = require "kong.cli.utils.logger"
local dao = require "kong.tools.dao_loader"
local lapp = require("lapp")

local args = lapp(string.format([[
For development purposes only.

Seed the database with random data or drop it.

Usage: kong db <command> [options]

Commands:
  <command> (string) where <command> is one of:
                       seed, drop

Options:
  -c,--config (default %s) path to configuration file
  -r,--random                              flag to also insert random entities
  -n,--number (default 1000)               number of random entities to insert if --random
]], constants.CLI.GLOBAL_KONG_CONF))

-- $ kong db
if args.command == "db" then
  lapp.quit("Missing required <command>.")
end

local parsed_config = require("kong.cli.services.nginx")(args.config).parsed_config
local dao_factory = dao.load(parsed_config)

if args.command == "seed" then

  -- Drop if exists
  local err = dao_factory:drop()
  if err then
    logger:error(err)
    os.exit(1)
  end

  local faker = Faker(dao_factory)
  faker:seed(args.random and args.number or nil)
  logger:success("Populated")

elseif args.command == "drop" then

  local err = dao_factory:drop()
  if err then
    logger:error(err)
    os.exit(1)
  end

  logger:success("Dropped")

else
  lapp.quit("Invalid command: "..args.command)
end
