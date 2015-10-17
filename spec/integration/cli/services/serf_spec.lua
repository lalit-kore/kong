require("kong.cli.utils.logger"):set_silent(true) -- Set silent for test

local spec_helper = require "spec.spec_helpers"
local configuration = require("kong.cli.utils.configuration").parse(spec_helper.get_env().conf_file)
local Serf = require("kong.cli.services.serf")(configuration.value)

describe("Serf", function()

  it("should start and stop", function()
    local ok, err = Serf:start()
    assert.truthy(ok)
    assert.falsy(err)

    assert.truthy(Serf:is_running())

    -- Trying again will fail
    local ok, err = Serf:start()
    assert.falsy(ok)
    assert.truthy(err)
    assert.equal("serf is already running", err)

    Serf:stop()

    assert.falsy(Serf:is_running())
  end)

    it("should stop even when not running", function()
    assert.falsy(Serf:is_running())
    Serf:stop()
    assert.falsy(Serf:is_running())
  end)

end)
