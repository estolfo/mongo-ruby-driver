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

      context 'when the operation fails on the first attempt' do

        before do
          # Note that for writes, server.connectable? is called, refreshing the socket
          allow(primary).to receive(:connectable?).and_return(true)
          expect(primary_socket).to receive(:write).and_raise(error)
        end

        context 'when the error is retryable' do

          before do
            expect(Mongo::Logger.logger).to receive(:warn).once
            expect(client.cluster).to receive(:scan!)
          end

          context 'when the error is a SocketError' do

            let(:error) do
              Mongo::Error::SocketError
            end

            it 'retries writes' do
              operation
              expect(collection.find(a: 1).count).to eq(1)
            end
          end

          context 'when the error is a SocketTimeoutError' do

            let(:error) do
              Mongo::Error::SocketTimeoutError
            end

            it 'retries writes' do
              operation
              expect(collection.find(a: 1).count).to eq(1)
            end
          end

          context 'when the error is a retryable OperationFailure' do

            let(:error) do
              Mongo::Error::OperationFailure.new('not master')
            end

            it 'retries writes' do
              operation
              expect(collection.find(a: 1).count).to eq(1)
            end
          end
        end

        context 'when the error is not retryable' do

          context 'when the error is a non-retryable OperationFailure' do

            let(:error) do
              Mongo::Error::OperationFailure.new('other error')
            end

            it 'does not retry writes' do
              expect {
                operation
              }.to raise_error(error)
              expect(collection.find(a: 1).count).to eq(0)
            end
          end
        end
      end

      context 'when the operation fails on the first attempt and again on the second attempt' do

        before do
          # Note that for writes, server.connectable? is called, refreshing the socket
          allow(primary).to receive(:connectable?).and_return(true)
          allow(primary_socket).to receive(:write).and_raise(error)
        end

        context 'when the selected server does not support retryable writes' do

          before do
            legacy_primary = double('legacy primary', :'retry_writes?' => false)
            allow(client.cluster).to receive(:next_primary).and_return(primary, legacy_primary)
            expect(primary_socket).to receive(:write).and_raise(error)
          end

          context 'when the error is a SocketError' do

            let(:error) do
              Mongo::Error::SocketError
            end

            it 'does not retry writes and raises the original error' do
              expect {
                operation
              }.to raise_error(error)
              expect(collection.find(a: 1).count).to eq(0)
            end
          end

          context 'when the error is a SocketTimeoutError' do

            let(:error) do
              Mongo::Error::SocketTimeoutError
            end

            it 'does not retry writes and raises the original error' do
              expect {
                operation
              }.to raise_error(error)
              expect(collection.find(a: 1).count).to eq(0)
            end
          end

          context 'when the error is a retryable OperationFailure' do

            let(:error) do
              Mongo::Error::OperationFailure.new('not master')
            end

            it 'does not retry writes and raises the original error' do
              expect {
                operation
              }.to raise_error(error)
              expect(collection.find(a: 1).count).to eq(0)
            end
          end
        end

        [Mongo::Error::SocketError, Mongo::Error::SocketTimeoutError].each do |retryable_error|

          context "when the first error is a #{retryable_error}" do

            let(:error) do
              retryable_error
            end

            before do
              bad_socket = primary_connection.address.socket(primary_connection.socket_timeout,
                                                             primary_connection.send(:ssl_options))
              good_socket = primary_connection.address.socket(primary_connection.socket_timeout,
                                                              primary_connection.send(:ssl_options))
              allow(bad_socket).to receive(:write).and_raise(second_error)
              allow(primary_connection.address).to receive(:socket).and_return(bad_socket, good_socket)
            end

            context 'when the second error is a SocketError' do

              let(:second_error) do
                Mongo::Error::SocketError
              end

              before do
                expect(client.cluster).to receive(:scan!).twice
              end

              it 'does not retry writes and raises the second error' do
                expect {
                  operation
                }.to raise_error(second_error)
                expect(collection.find(a: 1).count).to eq(0)
              end
            end

            context 'when the second error is a SocketTimeoutError' do

              before do
                expect(client.cluster).to receive(:scan!).twice
              end

              let(:second_error) do
                Mongo::Error::SocketTimeoutError
              end

              it 'does not retry writes and raises the second error' do
                expect {
                  operation
                }.to raise_error(second_error)
                expect(collection.find(a: 1).count).to eq(0)
              end
            end

            context 'when the second error is a retryable OperationFailure' do

              before do
                expect(client.cluster).to receive(:scan!).twice
              end

              let(:second_error) do
                Mongo::Error::OperationFailure.new('not master')
              end

              it 'does not retry writes and raises the second error' do
                expect {
                  operation
                }.to raise_error(second_error)
                expect(collection.find(a: 1).count).to eq(0)
              end
            end

            context 'when the second error is a non-retryable OperationFailure' do

              before do
                expect(client.cluster).to receive(:scan!).once
              end

              let(:second_error) do
                Mongo::Error::OperationFailure.new('other error')
              end

              it 'does not retry writes and raises the first error' do
                expect {
                  operation
                }.to raise_error(error)
                expect(collection.find(a: 1).count).to eq(0)
              end
            end

            context 'when the second error is a another error' do

              let(:second_error) do
                StandardError
              end

              it 'does not retry writes and raises the first error' do
                expect {
                  operation
                }.to raise_error(error)
                expect(collection.find(a: 1).count).to eq(0)
              end
            end
          end
        end
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
