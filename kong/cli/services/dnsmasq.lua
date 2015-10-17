local BaseService = require "kong.cli.services.base_service"
local logger = require "kong.cli.utils.logger"
local IO = require "kong.tools.io"

local Dnsmasq = BaseService:extend()

local SERVICE_NAME = "dnsmasq"

function Dnsmasq:new(configuration_value)
  self._parsed_config = configuration_value
  Dnsmasq.super.new(self, SERVICE_NAME, configuration_value.nginx_working_dir)
end

function Dnsmasq:start()
  if self._parsed_config.dns_resolver.dnsmasq then
    if self:is_running() then
      return nil, SERVICE_NAME.." is already running"
    end

    local cmd, err = Dnsmasq.super._get_cmd(self)
    if err then
      return nil, err
    end

    local res, code = IO.os_execute(cmd.." -p "..self._parsed_config.dns_resolver.port.." --pid-file="..self._pid_file_path.." -N -o")
    if code == 0 then
      setmetatable(self._parsed_config.dns_resolver, require "kong.tools.printable")
      logger:info(string.format([[dnsmasq ...........%s]], tostring(self._parsed_config.dns_resolver)))
      return true
    else
      return nil, res
    end
  end
end

function Dnsmasq:stop()
  if self._parsed_config.dns_resolver.dnsmasq then
    Dnsmasq.super.stop(self)
  end
end

return Dnsmasq