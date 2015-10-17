local crud = require "kong.api.crud_helpers"

return {
  ["/oauth2_tokens/"] = {
    GET = function(self, dao_factory, helpers)
      crud.paginated_set(self, dao_factory.oauth2_tokens)
    end
  },

  ["/oauth2_tokens/:id"] = {
    before = function(self, dao_factory, helpers)
      local err
      self.oauth2_token, err = dao_factory.oauth2_tokens:find_by_primary_key({ id = self.params.id })
      if err then
        return helpers.yield_error(err)
      elseif not self.oauth2_token then
        return helpers.responses.send_HTTP_NOT_FOUND()
      end
    end,

    GET = function(self, dao_factory, helpers)
      return helpers.responses.send_HTTP_OK(self.oauth2_token)
    end,

    PATCH = function(self, dao_factory)
      crud.patch(self.params, self.oauth2_token, dao_factory.oauth2_tokens)
    end,

    DELETE = function(self, dao_factory)
      crud.delete(self.oauth2_token, dao_factory.oauth2_tokens)
    end
  },

  ["/oauth2/"] = {
    GET = function(self, dao_factory, helpers)
      crud.paginated_set(self, dao_factory.oauth2_credentials)
    end
  },

  ["/consumers/:username_or_id/oauth2/"] = {
    before = function(self, dao_factory, helpers)
      crud.find_consumer_by_username_or_id(self, dao_factory, helpers)
      self.params.consumer_id = self.consumer.id
    end,

    GET = function(self, dao_factory, helpers)
      crud.paginated_set(self, dao_factory.oauth2_credentials)
    end,

    PUT = function(self, dao_factory)
      crud.put(self.params, dao_factory.oauth2_credentials)
    end,

    POST = function(self, dao_factory)
      crud.post(self.params, dao_factory.oauth2_credentials)
    end
  },

  ["/consumers/:username_or_id/oauth2/:id"] = {
    before = function(self, dao_factory, helpers)
     crud.find_consumer_by_username_or_id(self, dao_factory, helpers)
      self.params.consumer_id = self.consumer.id

      local data, err = dao_factory.oauth2_credentials:find_by_keys({ id = self.params.id })
      if err then
        return helpers.yield_error(err)
      end

      self.credential = data[1]
      if not self.credential then
        return helpers.responses.send_HTTP_NOT_FOUND()
      end
    end,

    GET = function(self, dao_factory, helpers)
      return helpers.responses.send_HTTP_OK(self.credential)
    end,

    PATCH = function(self, dao_factory)
      crud.patch(self.params, self.credential, dao_factory.oauth2_credentials)
    end,

    DELETE = function(self, dao_factory)
      crud.delete(self.credential, dao_factory.oauth2_credentials)
    end
  }
}
