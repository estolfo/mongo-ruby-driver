module Mongo
  module Operations
    class Update

      module Builder

        class Command
          extend Forwardable

          attr_reader :operation
          attr_reader :server

          def_delegators :operation, :write_concern, :collation, :bypass_document_validation

          def initialize(operation, server)
            @operation = operation
            @server = server
          end

          def selector
            { update: operation.coll_name,
              updates: operation.updates
            }.merge(command_options)
          end

          def options
            opts = { ordered: ordered? }
            opts[:writeConcern] = write_concern.options if write_concern
            opts[:collation] = collation if collation
            opts[:bypassDocumentValidation] = true if bypass_document_validation
            opts[:limit] = 1
            opts
          end

          def message
            Protocol::Query.new(operation.db_name, Database::COMMAND, selector, options)
          end
        end
      end
    end
  end
end
