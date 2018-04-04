
require 'mongo/operations/list_collections/builder/command'
require 'mongo/operations/list_collections/builder/op_msg'
require 'mongo/operations/list_collections/result'

module Mongo
  module Operations

    class ListCollections
      include Operations::Specifiable

      def initialize(spec)
        @spec = spec
      end

      def selector
        (spec[SELECTOR] || {}).merge(
            listCollections: 1, filter: { name: { '$not' => /system\.|\$/ }})
      end

      def execute(server)
        if server.features.op_msg_enabled?
          message = Builder::OpMsg.new(self, server).message
        else
          message = Builder::Command.new(self, server).message
        end

        server.with_connection do |connection|
          reply = connection.dispatch([ message ], operation_id)
          result = Result.new(reply)
          server.update_cluster_time(result)
          session.process(result) if session
          result.validate!
        end
      end
    end
  end
end
