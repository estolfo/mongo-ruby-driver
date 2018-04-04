module Mongo
  module Operations
    class Update

      module Builder

        class OpMsg
          extend Forwardable

          attr_reader :operation
          attr_reader :server

          def_delegators :operation, :read, :write_concern, :acknowledged_write?, :session, :txn_num

          ZERO_TIMESTAMP = BSON::Timestamp.new(0,0)

          def initialize(operation, server)
            @operation = operation
            @server = server
          end

          def flags
            acknowledged_write? ? [:none] : [:more_to_come]
          end

          def apply_cluster_time!(selector, server)
            if !server.standalone?
              cluster_time = [ server.cluster_time, (session && session.cluster_time) ].max_by do |doc|
                (doc && doc[Cluster::CLUSTER_TIME]) || ZERO_TIMESTAMP
              end

              if cluster_time && (cluster_time[Cluster::CLUSTER_TIME] > ZERO_TIMESTAMP)
                selector[CLUSTER_TIME] = cluster_time
              end
            end
          end

          def apply_session_id!(selector)
            session.add_id!(selector)
          end

          def update_selector_for_session!(selector, server)
            if server.features.sessions_enabled?
              apply_cluster_time!(selector, server)
              if acknowledged_write? && session
                selector[:txnNumber] = BSON::Int64.new(txn_num) if txn_num
                apply_session_id!(selector)
                #apply_causal_consistency!(selector, server)
              end
            elsif session && session.explicit?
              apply_cluster_time!(selector, server)
              selector[:txnNumber] = BSON::Int64.new(txn_num) if txn_num
              apply_session_id!(selector)
              #apply_causal_consistency!(selector, server)
            end
          end

          def global_args
            { update: coll_name,
              Protocol::Msg::DATABASE_IDENTIFIER => db_name
            }.merge!(command_options)
          end

          def message
            selector = global_args
            update_selector_for_session!(selector, server)
            section = { type: 1, payload: { identifier: IDENTIFIER, sequence: updates } }
            flags = acknowledged_write? ? [:none] : [:more_to_come]
            Protocol::Msg.new(flags, {}, selector, section)
          end
        end
      end
    end
  end
end
