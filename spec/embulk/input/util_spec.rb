require 'embulk/input/util'

RSpec.describe Util do
  subject { Util.filter_entities_by_time_string(entities, start_date, end_date, timezone) }

  describe 'date' do
    let(:timezone) { 'UTC' }

    let(:entities) { [entity_0101, entity_0102, entity_0103] }
    let(:entity_0101) { {'created_at' => '2020-01-01T00:00Z'} }
    let(:entity_0102) { {'created_at' => '2020-01-02T00:00Z'} }
    let(:entity_0103) { {'created_at' => '2020-01-03T00:00Z'} }

    context 'start_date and end_date != nil' do
      let(:start_date) { '2020-01-02' }
      let(:end_date) { '2020-01-02' }

      it { is_expected.to eq([entity_0102]) }
    end

    context 'only start_date != nil' do
      let(:start_date) { '2020-01-02' }
      let(:end_date) { nil }

      it { is_expected.to eq([entity_0102, entity_0103]) }
    end

    context 'only end_date != nil' do
      let(:start_date) { nil }
      let(:end_date) { '2020-01-02' }

      it { is_expected.to eq([entity_0101, entity_0102]) }
    end

    context 'start_date and end_date = nil' do
      let(:start_date) { nil }
      let(:end_date) { nil }

      it { is_expected.to eq([entity_0101, entity_0102, entity_0103]) }
    end

    context 'start_date invalid' do
      let(:start_date) { 'invalid' }
      let(:end_date) { nil }

      it { expect { subject }.to raise_error(ArgumentError) }
    end

    context 'end_date invalid' do
      let(:start_date) { nil }
      let(:end_date) { 'invalid' }

      it { expect { subject }.to raise_error(ArgumentError) }
    end
  end

  describe 'timezone' do
    let(:start_date) { '2020-01-02' }
    let(:end_date) { '2020-01-02' }

    let(:entities) { [entity_utc, entity_tokyo] }
    let(:entity_utc) { {'created_at' => '2020-01-02T00:00Z'} }
    let(:entity_tokyo) { {'created_at' => '2020-01-01T15:00Z'} }

    context 'timezone = UTC' do
      let(:timezone) { 'UTC' }

      it { is_expected.to eq([entity_utc]) }
    end

    context 'timezone = Asia/Tokyo' do
      let(:timezone) { 'Asia/Tokyo' }

      it { is_expected.to eq([entity_tokyo]) }
    end

    context 'timezone = nil' do
      let(:timezone) { nil }

      it { is_expected.to eq([entity_utc, entity_tokyo]) }
    end

    context 'timezone invalid' do
      let(:timezone) { 'invalid' }

      it { expect { subject }.to raise_error(ArgumentError) }
    end
  end
end
