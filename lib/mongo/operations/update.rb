
require 'pry-nav'
require 'mongo/operations/update/builder/command'
require 'mongo/operations/update/builder/op_msg'
require 'mongo/operations/update/legacy/result'
require 'mongo/operations/update/legacy/update'

module Mongo
  module Operations

    class Update
      include Operations::Specifiable

      def initialize(spec)
        @spec = spec
      end

      def acknowledged_write?
        write_concern.nil? || write_concern.acknowledged?
      end

      def has_array_filters?
        update[Operation::ARRAY_FILTERS]
      end

      def has_collation?
        update[:collation] || update[Operation::COLLATION]
      end

      def validate!
        if unacknowledged_write?
          if has_collation?
            raise Error::UnsupportedCollation.new(Error::UnsupportedCollation::UNACKNOWLEDGED_WRITES_MESSAGE)
          end
          if has_array_filters?
            raise Error::UnsupportedArrayFilters.new(Error::UnsupportedArrayFilters::UNACKNOWLEDGED_WRITES_MESSAGE)
          end
        end
      end

      def execute(server)
        validate!
        if server.features.op_msg_enabled?
          message = Builder::OpMsg.new(self, server).message
        elsif unacknowledged_write?
          Legacy::Update.new(spec).execute(server)
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
