
require 'pry-nav'
require 'mongo/operations/aggregate/builder/command'
require 'mongo/operations/aggregate/builder/op_msg'
require 'mongo/operations/aggregate/result'

module Mongo
  module Operations

    class Aggregate
      include Operations::Specifiable

      def initialize(spec)
        @spec = spec
      end

      def acknowledged_write?
        write_concern.nil? || write_concern.acknowledged?
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

