
require 'pry-nav'
require 'mongo/operations/update/builder/command'
require 'mongo/operations/update/builder/op_msg'
require 'mongo/operations/update/builder/message'

module Mongo
  module Operations
    module Legacy
      class Update
        include Operations::Specifiable

        def initialize(spec)
          @spec = spec
        end

        def acknowledged_write?
          write_concern.nil? || write_concern.acknowledged?
        end

        def execute(server)
          server.with_connection do |connection|
            reply = connection.dispatch([ message, gle ], operation_id)
            result = Result.new(reply)
            server.update_cluster_time(result)
            session.process(result) if session
            result.validate!
          end
        end

        def message
          flags = []
          flags << :multi_update if update[Operation::MULTI]
          flags << :upsert if update[Operation::UPSERT]

          Protocol::Update.new(
              db_name,
              coll_name,
              update[Operation::Q],
              update[Operation::U],
              flags.empty? ? {} : { flags: flags }
          )
        end

        def gle
          wc = write_concern ||  WriteConcern.get(WriteConcern::DEFAULT)
          if gle_message = wc.get_last_error
            Protocol::Query.new(
                db_name,
                Database::COMMAND,
                gle_message,
                options.merge(limit: -1)
            )
          end
        end
      end
    end
  end
end
