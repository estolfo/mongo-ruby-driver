require 'spec_helper'

describe Mongo::Session::ServerSession do

  describe '#initialize' do

    it 'sets the last use variable to the current time' do
      expect(described_class.new.last_use).to be_within(0.2).of(Time.now)
    end

    it 'sets a UUID as the session id' do
      expect(described_class.new.instance_variable_get(:@session_id)).to be_a(BSON::Document)
      expect(described_class.new.session_id).to be_a(BSON::Document)
      expect(described_class.new.session_id[:id]).to be_a(BSON::Binary)
    end
  end

  describe '#next_txn_number' do

    it 'advances and returns the next transaction number' do
      expect(described_class.new.next_txn_num).to be(0)
    end

    context 'when the method is called multiple times' do

      let(:server_session) do
        described_class.new
      end

      before do
        server_session.next_txn_num
        server_session.next_txn_num
      end

      it 'advances and returns the next transaction number' do
        expect(server_session.next_txn_num).to be(2)
      end
    end
  end
end
