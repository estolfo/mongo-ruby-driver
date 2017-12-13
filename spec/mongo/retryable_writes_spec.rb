require 'spec_helper'

describe 'Retryable Writes' do

  RETRYABLE_WRITES_TESTS.each do |file|

    spec = Mongo::CRUD::Spec.new(file)

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
            test.setup_test(collection)
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

          it 'has the correct data in the collection', if: (sessions_enabled? && replica_set? && test.outcome_collection_data) do
            skip 'Test cannot be run on this server version' unless spec.server_version_satisfied?(client)
            results
            expect(collection.find.to_a).to match_collection_data(test)
          end

          if test.error?
            it 'raises an error', if: sessions_enabled? && replica_set? do
              skip 'Test cannot be run on this server version' unless spec.server_version_satisfied?(client)
              expect(results).to be_a(Mongo::Error)
            end
          else
            it 'returns the correct result', if: sessions_enabled? && replica_set? do
              skip 'Test cannot be run on this server version' unless spec.server_version_satisfied?(client)
              expect(results).to match_operation_result(test)
            end
          end
        end
      end
    end
  end

  describe 'Retryable writes integration tests' do

    let(:primary) do
      primary = client.cluster.next_primary
    end

    let(:primary_connection) do
      connection = primary.pool.checkout
      connection.connect!
      primary.pool.checkin(connection)
      connection
    end

    let(:primary_socket) do
      primary_connection.send(:socket)
    end

    after do
      authorized_collection.delete_many
    end

    let(:operation) do
      collection.insert_one(a:1)
    end

    shared_examples_for 'an operation that is retried' do

      before do
        # Note that for writes, server.connectable? is called, refreshing the socket
        allow(primary).to receive(:connectable?).and_return(true)
        expect(primary_socket).to receive(:write).and_raise(Mongo::Error::SocketError)
        expect(client.cluster).to receive(:scan!)
      end

      it 'retries writes', if: !standalone? || !sessions_enabled? do
        operation
        expect(collection.find(a: 1).count).to eq(1)
      end
    end

    shared_examples_for 'an operation that is not retried' do

      before do
        # Note that for writes, server.connectable? is called, refreshing the socket
        allow(primary).to receive(:connectable?).and_return(true)
        expect(primary_socket).to receive(:write).and_raise(Mongo::Error::SocketError)
        expect(client.cluster).not_to receive(:scan!)
      end

      it 'does not retry writes' do
        expect {
          operation
        }.to raise_error(Mongo::Error::SocketError)
        expect(collection.find(a: 1).count).to eq(0)
      end
    end

    context 'when the client has retry_writes set to true' do

      let!(:client) do
        authorized_client.with(retry_writes: true)
      end

      context 'when the collection has write concern acknowledged' do

        let!(:collection) do
          client[TEST_COLL, write: WRITE_CONCERN]
        end

        context 'when the server supports retryable writes' do

          before do
            allow(primary).to receive(:retry_writes?).and_return(true)
          end

          if standalone? || !sessions_enabled?
            it_behaves_like 'an operation that is not retried'
          else
            it_behaves_like 'an operation that is retried'
          end
        end

        context 'when the server does not support retryable writes' do

          before do
            allow(primary).to receive(:retry_writes?).and_return(false)
          end

          it_behaves_like 'an operation that is not retried'
        end
      end

      context 'when the collection has write concern unacknowledged' do

        let!(:collection) do
          client[TEST_COLL, write: { w: 0 }]
        end

        it_behaves_like 'an operation that is not retried'
      end

      context 'when the collection has write concern not set' do

        let!(:collection) do
          client[TEST_COLL]
        end

        context 'when the server supports retryable writes' do

          before do
            allow(primary).to receive(:retry_writes?).and_return(true)
          end

          if standalone?
            it_behaves_like 'an operation that is not retried'
          else
            it_behaves_like 'an operation that is retried'
          end
        end

        context 'when the server does not support retryable writes' do

          before do
            allow(primary).to receive(:retry_writes?).and_return(false)
          end

          it_behaves_like 'an operation that is not retried'
        end
      end
    end

    context 'when the client has retry_writes set to false' do

      let!(:client) do
        authorized_client.with(retry_writes: false)
      end

      context 'when the collection has write concern acknowledged' do

        let!(:collection) do
          client[TEST_COLL, write: WRITE_CONCERN]
        end

        it_behaves_like 'an operation that is not retried'
      end

      context 'when the collection has write concern unacknowledged' do

        let!(:collection) do
          client[TEST_COLL, write: { w: 0 }]
        end

        it_behaves_like 'an operation that is not retried'
      end

      context 'when the collection has write concern not set' do

        let!(:collection) do
          client[TEST_COLL]
        end

        it_behaves_like 'an operation that is not retried'
      end
    end

    context 'when the client has retry_writes not set' do

      let!(:client) do
        authorized_client
      end

      context 'when the collection has write concern acknowledged' do

        let!(:collection) do
          client[TEST_COLL, write: WRITE_CONCERN]
        end

        it_behaves_like 'an operation that is not retried'
      end

      context 'when the collection has write concern unacknowledged' do

        let!(:collection) do
          client[TEST_COLL, write: { w: 0 }]
        end

        it_behaves_like 'an operation that is not retried'
      end

      context 'when the collection has write concern not set' do

        let!(:collection) do
          client[TEST_COLL]
        end

        it_behaves_like 'an operation that is not retried'
      end
    end
  end
end
