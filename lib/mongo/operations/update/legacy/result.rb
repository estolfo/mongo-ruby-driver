module Mongo
  module Operations
    module Legacy
      class Update

        # Defines custom behaviour of results for an update on server
        # version <= 2.4.
        #
        # @since 2.0.0
        class Result < Operation::Result

          # Whether an existing document was updated.
          #
          # @since 2.0.0
          UPDATED_EXISTING = 'updatedExisting'.freeze

          # The upserted docs field in the result.
          #
          # @since 2.0.0
          UPSERTED = 'upserted'.freeze

          # Get the number of documents matched.
          #
          # @example Get the matched count.
          #   result.matched_count
          #
          # @return [ Integer ] The matched count.
          #
          # @since 2.0.0
          def matched_count
            return 0 unless acknowledged?
            if upsert?
              0
            else
              n
            end
          end

          # Get the number of documents modified.
          #
          # @example Get the modified count.
          #   result.modified_count
          #
          # @return [ nil ] Always omitted for legacy versions.
          #
          # @since 2.0.0
          def modified_count; end

          # The identifier of the inserted document if an upsert
          #   took place.
          #
          # @example Get the upserted document's identifier.
          #   result.upserted_id
          #
          # @return [ Object ] The upserted id.
          #
          # @since 2.0.0
          def upserted_id
            first[UPSERTED] if upsert?
          end

          # Returns the number of documents upserted.
          #
          # @example Get the number of upserted documents.
          #   result.upserted_count
          #
          # @return [ Integer ] The number upserted.
          #
          # @since 2.4.2
          def upserted_count
            upsert? ? n : 0
          end

          private

          def upsert?
            !updated_existing? && n == 1
          end

          def updated_existing?
            first[UPDATED_EXISTING]
          end
        end
      end
    end
  end
end
