
require 'mongo/operations/collections_info/builder/command'
require 'mongo/operations/collections_info/result'

module Mongo
  module Operations

    class CollectionsInfo
      include Operations::Specifiable

      def initialize(spec)
        @spec = spec
      end

      def selector
        { :name => { '$not' => /system\.|\$/ } }
      end

      def execute(server)
        if server.features.list_collections_enabled?
          return Operations::ListCollections.new(spec).execute(server)
        end

        message = Builder::Command.new(self, server).message

        server.with_connection do |connection|
          reply = connection.dispatch([message], operation_id)
          result = Result.new(reply)
          server.update_cluster_time(result)
          session.process(result) if session
          result.validate!
        end
      end
    end
  end
end
