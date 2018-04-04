module Mongo
  module Operations
    class Command

      module Builder

        class Command
          extend Forwardable

          attr_reader :operation
          attr_reader :server

          def_delegators :operation, :read

          # The constant for slave ok flags.
          #
          # @since 2.0.6
          SLAVE_OK = :slave_ok

          def initialize(operation, server)
            @operation = operation
            @server = server
          end

          def selector
            update_selector_for_read_pref(operation.selector.dup)
          end

          def options
            opts = update_options_for_slave_ok(operation.options, server)
            opts.merge(limit: 1)
          end

          def update_selector_for_read_pref(sel)
            if read && server.mongos? && read_pref = read.to_mongos
              sel = sel[:$query] ? sel : { :$query => sel }
              sel.merge(:$readPreference => read_pref)
            else
              sel
            end
          end

          def slave_ok?(server)
            (server.cluster.single? && !server.mongos?) || (read && read.slave_ok?)
          end

          def update_options_for_slave_ok(opts, server)
            if slave_ok?(server)
              opts.dup.tap do |o|
                (o[:flags] ||= []) << SLAVE_OK
              end
            else
              opts
            end
          end

          def message
            Protocol::Query.new(operation.db_name, Database::COMMAND, selector, options)
          end
        end
      end
    end
  end
end
