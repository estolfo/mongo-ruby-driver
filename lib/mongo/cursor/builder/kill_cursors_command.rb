# Copyright (C) 2015-2017 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Cursor
    module Builder

      # Generates a specification for a kill cursors command.
      #
      # @since 2.2.0
      class KillCursorsCommand
        extend Forwardable

        # @return [ Cursor ] cursor The cursor.
        attr_reader :cursor

        def_delegators :@cursor, :collection_name, :database

        # Create the new builder.
        #
        # @example Create the builder.
        #   KillCursorsCommand.new(cursor)
        #
        # @param [ Cursor ] cursor The cursor.
        #
        # @since 2.2.0
        def initialize(cursor, session = nil)
          @cursor = cursor
          @session = session
        end

        # Get the specification.
        #
        # @example Get the specification.
        #   kill_cursors_command.specification
        #
        # @return [ Hash ] The spec.
        #
        # @since 2.2.0
        def specification
          { selector: kill_cursors_command, db_name: database.name }
        end

        private

        def kill_cursors_command
          # add session id
          cmd = { :killCursors => collection_name, :cursors => [ cursor.id ] }
          @session ? @session.add_id(cmd) : cmd
        end

        class << self

          # Update a specification's list of cursor ids.
          #
          # @example Update a specification's list of cursor ids.
          #   KillCursorsCommand.update_cursors(spec, ids)
          #
          # @return [ Hash ] The specification.
          # @return [ Array ] The ids to update with.
          #
          # @since 2.3.0
          def update_cursors(spec, ids)
            spec[:selector].merge!(cursors: spec[:selector][:cursors] & ids)
          end

          # Get the list of cursor ids from a spec generated by this Builder.
          #
          # @example Get the list of cursor ids.
          #   KillCursorsCommand.cursors(spec)
          #
          # @return [ Hash ] The specification.
          #
          # @since 2.3.0
          def get_cursors_list(spec)
            spec[:selector][:cursors]
          end
        end
      end
    end
  end
end
