local events = require "kong.core.events"
local cache = require "kong.tools.database_cache"
local stringy = require "stringy"
local cjson = require "cjson"

local function invalidate_plugin(entity)
  cache.delete(cache.plugin_key(entity.name, entity.api_id, entity.consumer_id))
end

local function invalidate(message_t)
  if message_t.collection == "consumers" then
    cache.delete(cache.consumer_key(message_t.entity.id))
  elseif message_t.collection == "apis" then
    if message_t.entity then
      cache.delete(cache.api_key(message_t.entity.id))
    end
    cache.delete(cache.all_apis_by_dict_key())
  elseif message_t.collection == "plugins" then
    -- Handles both the update and the delete
    invalidate_plugin(message_t.old_entity and message_t.old_entity or message_t.entity)
  end
end

local function member_event(message_t)
  -- On every membership event, resync the nodes list
  local serf = require("kong.cli.services.serf")(configuration)
  local res, err = serf:invoke_signal("members", { ["-format"] = "json" })
  if err then
    ngx.log(ngx.ERR, err)
  else

    -- Update all the existing nodes, or insert missing ones
    local members = cjson.decode(res).members
    for _, member in ipairs(members) do
      local nodes, err = dao.nodes:find_by_keys({
        address = member.addr
      })
      if err then
        ngx.log(ngx.ERR, tostring(err))
        return
      end
      if #nodes == 0 then
        dao.nodes:insert({
          name = stringy.strip(member.name),
          address = stringy.strip(member.addr),
          status = stringy.strip(member.status),
          tags = member.tags
        })
      elseif #nodes == 1 then
        local node = table.remove(nodes, 1)
        node.name = member.name
        node.status = member.status
        node.tags = member.tags
        local _, err = dao.nodes:update(node)
        if err then
          ngx.log(ngx.ERR, tostring(err))
          return
        end
      end
    end

    -- Remove members that don't exist anymore
    local nodes, err = dao.nodes:find_all()
    if err then
      ngx.log(ngx.ERR, tostring(err))
      return
    end
    for _, node in ipairs(nodes) do
      local found
      for _, member in ipairs(members) do 
        if member.addr == node.address then
          found = true
          break
        end
      end
      if not found then
        local _, err = dao.nodes:delete({
          name = node.name
        })
        if err then
          ngx.log(ngx.ERR, tostring(err))
        end
      end
    end

  end
end

return {
  [events.TYPES.ENTITY_UPDATED] = function(message_t)
    invalidate(message_t)
  end,
  [events.TYPES.ENTITY_DELETED] = function(message_t)
    invalidate(message_t)
  end,
  [events.TYPES.ENTITY_CREATED] = function(message_t)
    invalidate(message_t)
  end,
  [events.TYPES.CLUSTER_PROPAGATE] = function(message_t)
    local serf = require("kong.cli.services.serf")(configuration)
    local ok, err = serf:event(message_t)
    if not ok then
      ngx.log(ngx.ERR, err)
    end
  end,
  [events.TYPES["MEMBER-JOIN"]] = function(message_t)
    member_event(message_t)
  end,
  [events.TYPES["MEMBER-LEAVE"]] = function(message_t)
    member_event(message_t)
  end,
  [events.TYPES["MEMBER-FAILED"]] = function(message_t)
    member_event(message_t)
  end,
  [events.TYPES["MEMBER-UPDATE"]] = function(message_t)
    member_event(message_t)
  end,
  [events.TYPES["MEMBER-REAP"]] = function(message_t)
    member_event(message_t)
  end
}