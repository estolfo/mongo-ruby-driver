require 'spec_helper'

describe 'Retryable Writes' do

  RETRYABLE_WRITES_TESTS.each do |file|

    spec = Mongo::CRUD::Spec.new(file)

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do

          before(:each) do
            authorized_collection.delete_many
          end

          let(:client) do
            authorized_client.with(retry_writes: true)
          end

          after(:each) do
            authorized_client.use(:admin).command(configureFailPoint: "onPrimaryTransactionalWrite", mode: "off" )
            authorized_collection.delete_many
          end

          let(:results) do
            if test.error?
              error = nil
              begin; test.run(client[authorized_collection.name]); rescue => e; error = e; end
              error
            else
              test.run(client[authorized_collection.name])
            end
          end

          # it 'returns the correct result' do
          #   skip 'Test cannot be run on this server version' unless spec.server_version_satisfied?(authorized_client)
          #   expect(results).to match_operation_result(test)
          # end

          it 'has the correct data in the collection', if: test.outcome_collection_data do
            skip 'Test cannot be run on this server version' unless spec.server_version_satisfied?(authorized_client)
            results
            binding.pry
            expect(authorized_collection.find.to_a).to match_collection_data(test)
          end
        end
      end
    end
  end
end
