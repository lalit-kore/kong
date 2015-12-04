local BaseDao = require "kong.dao.cassandra.base_dao"
local nodes_schema = require "kong.dao.schemas.nodes"
local query_builder = require "kong.dao.cassandra.query_builder"
local cjson = require "cjson"

local Nodes = BaseDao:extend()

function Nodes:new(properties, events_handler)
  self._table = "nodes"
  self._schema = nodes_schema
  Nodes.super.new(self, properties, events_handler)
end

function Nodes:find_all()
  local nodes = {}
  local select_q = query_builder.select(self._table)
  for rows, err in Nodes.super.execute(self, select_q, nil, nil, {auto_paging=true}) do
    if err then
      return nil, err
    end

    for _, row in ipairs(rows) do
      table.insert(nodes, row)
    end
  end

  return nodes
end

return {nodes = Nodes}
