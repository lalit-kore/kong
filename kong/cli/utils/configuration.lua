local IO = require "kong.tools.io"
local logger = require "kong.cli.utils.logger"
local luarocks = require "kong.cli.utils.luarocks"
local config = require "kong.tools.config_loader"

local _M = {}

function _M.parse(kong_config_path)
  if not IO.file_exists(kong_config_path) then
    logger:warn("No configuration at: "..kong_config_path.." using default config instead.")
    kong_config_path = IO.path:join(luarocks.get_config_dir(), "kong.yml")
  end

  logger:info("Using configuration: "..kong_config_path)

  if not IO.file_exists(kong_config_path) then
    return false, "No configuration at: "..kong_config_path
  end

  return {
    value = config.load(kong_config_path),
    path = kong_config_path
  }
end

return _M