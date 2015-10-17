local logger = require "kong.cli.utils.logger"
local dao = require "kong.tools.dao_loader"

local _M = {}

-- Services ordered by priority
local services = {
  require "kong.cli.services.serf",
  require "kong.cli.services.dnsmasq",
  require "kong.cli.services.nginx"
}

local function prepare_database(parsed_config)
  setmetatable(parsed_config.dao_config, require "kong.tools.printable")
  logger:info(string.format([[database...........%s %s]], parsed_config.database, tostring(parsed_config.dao_config)))

  local dao_factory = dao.load(parsed_config)
  local migrations = require("kong.tools.migrations")(dao_factory)

  local keyspace_exists, err = dao_factory.migrations:keyspace_exists()
  if err then
    return false, err
  elseif not keyspace_exists then
    logger:info("Database not initialized. Running migrations...")
  end

  local err = migrations:migrate_all(parsed_config, function(identifier, migration)
    if migration then
      logger:success(string.format("%s migrated up to: %s", identifier, logger.colors.yellow(migration.name)))
    end
  end)
  if err then
    return false, err
  end

  return true
end

function _M.stop_all(configuration)
  -- Stop in reverse order to keep dependencies running
  for index = #services,1,-1 do
    services[index](configuration.value, configuration.path):stop()
  end
end

function _M.start_all(configuration)

  -- Prepare database if not initialized yet
  local _, err = prepare_database(configuration.value)
  if err then
    return false, err
  end

  for _, v in ipairs(services) do
    local obj = v(configuration.value, configuration.path)
    obj:prepare()
    local ok, err = obj:start()
    if not ok then
      return ok, err
    end
  end

  return true
end

return _M