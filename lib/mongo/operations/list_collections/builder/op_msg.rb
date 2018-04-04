module Mongo
  module Operations
    class ListCollections

      module Builder

        class OpMsg
          extend Forwardable

          attr_reader :operation
          attr_reader :server

          def_delegators :operation, :read, :session, :txn_num

          ZERO_TIMESTAMP = BSON::Timestamp.new(0,0)

          READ_PREFERENCE = '$readPreference'.freeze

          def initialize(operation, server)
            @operation = operation
            @server = server
          end

          def selector
            sel = operation.selector.dup
            update_selector_for_session!(sel, server)
            sel[Protocol::Msg::DATABASE_IDENTIFIER] = operation.db_name
            sel[READ_PREFERENCE] = read.to_doc if read
            sel
          end

          def flags
            [:none]
          end

          def options
            operation.options
          end

          def apply_causal_consistency!(selector, server); end

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
              if session
                selector[:txnNumber] = BSON::Int64.new(txn_num) if txn_num
                apply_session_id!(selector)
                apply_causal_consistency!(selector, server)
              end
            elsif session && session.explicit?
              apply_cluster_time!(selector, server)
              selector[:txnNumber] = BSON::Int64.new(txn_num) if txn_num
              apply_session_id!(selector)
              apply_causal_consistency!(selector, server)
            end
          end

          def message
            Protocol::Msg.new(flags, options, selector)
          end
        end
      end
    end
  end
end
