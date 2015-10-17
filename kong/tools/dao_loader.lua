local _M = {}

function _M.load(parsed_config, events_handler)
  local DaoFactory = require("kong.dao."..parsed_config.database..".factory")
  return DaoFactory(parsed_config.dao_config, parsed_config.plugins_available, events_handler)
end

return _M
