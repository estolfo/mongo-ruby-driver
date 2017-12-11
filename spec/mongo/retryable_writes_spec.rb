require 'spec_helper'

describe 'Retryable Writes' do

  RETRYABLE_WRITES_TESTS.each do |file|

    spec = Mongo::CRUD::Spec.new(file)

    before(:all) do
      #{setFeatureCompatibilityVersion: 3.6}
    end

    context(spec.description) do

      spec.tests.each do |test|

        context(test.description) do

          let(:collection) do
            client[TEST_COLL]
          end

          let(:client) do
            authorized_client.with(heartbeat_frequency: 100, retry_writes: true).tap do |cl|
              cl.subscribe(Mongo::Monitoring::COMMAND, subscriber)
            end
          end

          let(:subscriber) do
            EventSubscriber.new
          end

          before do
            test.setup_test(authorized_collection)
          end

          after do
            test.clear_fail_point(collection)
            collection.delete_many
          end

          let(:results) do
            if test.error?
              error = nil
              begin; test.run(collection); rescue => e; error = e; end
              error
            else
              test.run(collection)
            end
          end

          it 'has the correct data in the collection', if: test.outcome_collection_data do
            skip 'Test cannot be run on this server version' unless spec.server_version_satisfied?(client)
            results
            expect(collection.find.to_a).to match_collection_data(test)
          end

          if test.error?
            it 'raises an error' do
              skip 'Test cannot be run on this server version' unless spec.server_version_satisfied?(client)
              expect(results).to be_a(Mongo::Error)
            end
          else
            it 'returns the correct result' do
              skip 'Test cannot be run on this server version' unless spec.server_version_satisfied?(client)
              expect(results).to match_operation_result(test)
            end
          end
        end
      end
    end
  end
end
