require("kong.cli.utils.logger"):set_silent(true) -- Set silent for test

local spec_helper = require "spec.spec_helpers"
local configuration = require("kong.cli.utils.configuration").parse(spec_helper.get_env().conf_file)
local Dnsmasq = require("kong.cli.services.dnsmasq")(configuration.value)

describe("Dnsmasq", function()

  it("should start and stop", function()
    local ok, err = Dnsmasq:start()
    assert.truthy(ok)
    assert.falsy(err)

    assert.truthy(Dnsmasq:is_running())

    -- Trying again will fail
    local ok, err = Dnsmasq:start()
    assert.falsy(ok)
    assert.truthy(err)
    assert.equal("dnsmasq is already running", err)

    Dnsmasq:stop()

    assert.falsy(Dnsmasq:is_running())
  end)

  it("should stop even when not running", function()
    assert.falsy(Dnsmasq:is_running())
    Dnsmasq:stop()
    assert.falsy(Dnsmasq:is_running())
  end)

end)
