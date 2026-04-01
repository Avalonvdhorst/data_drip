# frozen_string_literal: true

module DataDrip
  class BackfillRun < ApplicationRecord
    self.table_name = "data_drip_backfill_runs"

    has_many :batches,
             class_name: "DataDrip::BackfillRunBatch",
             dependent: :destroy
    belongs_to :backfiller, class_name: DataDrip.backfiller_class

    validates :backfill_class_name, presence: true
    validate :backfill_class_exists
    validate :backfill_class_properly_configured?
    validate :validate_scope, on: :create
    validate :start_at_must_be_valid_datetime
    validates :start_at, presence: true
    validates :batch_size, presence: true, numericality: { greater_than: 0 }
    validates :amount_of_elements,
              numericality: {
                greater_than_or_equal_to: 0
              },
              allow_nil: true

    after_commit :enqueue
    after_commit :run_hooks

    DataDrip.cross_rails_enum(
      self,
      :status,
      %i[pending enqueued running completed failed stopped]
    )

    def backfiller_name
      @backfiller_name ||=
        backfiller.send(DataDrip.backfiller_name_attribute.to_sym)
    end

    def backfill_class
      @backfill_class ||=
        DataDrip.all.find { |klass| klass.name == backfill_class_name }
    end

    def insight_run_duration_seconds
      time_window = insight_processing_time_window
      return nil unless time_window

      elapsed_seconds = (time_window.last - time_window.first).to_f
      elapsed_seconds.positive? ? elapsed_seconds : nil
    end

    def insight_average_batch_duration_seconds
      run_duration_seconds = insight_run_duration_seconds
      return nil unless run_duration_seconds&.positive?

      batch_count = batches.count
      return nil if batch_count.zero?

      run_duration_seconds / batch_count
    end

    def insight_elements_per_second
      run_duration_seconds = insight_run_duration_seconds
      return nil unless run_duration_seconds&.positive?

      processed_elements_count = processed_count.to_i
      return nil if processed_elements_count.zero?

      processed_elements_count / run_duration_seconds
    end

    def enqueue
      return unless pending?

      DataDrip::Dripper.set(wait_until: start_at).perform_later(self)
      enqueued!
    end

    private

    def insight_processing_time_window
      return nil unless batches.exists?

      start_time = batches.minimum(:created_at)
      return nil unless start_time

      end_time =
        if running?
          Time.current
        else
          batches.maximum(:updated_at)
        end

      [ start_time, end_time ]
    end

    def run_hooks
      return unless status_previously_changed?

      hook_name = "on_run_#{status}"
      if backfill_class.respond_to?(hook_name)
        backfill_class.send(hook_name, self)
      elsif DataDrip.hooks_handler_class.present? && DataDrip.hooks_handler_class.respond_to?(hook_name)
        DataDrip.hooks_handler_class.send(hook_name, self)
      end
    end

    def backfill_class_exists
      return if backfill_class

      errors.add(
        :backfill_class_name,
        "must be a valid DataDrip backfill class"
      )
    end

    def backfill_class_properly_configured?
      return unless backfill_class

      return if backfill_class < DataDrip::Backfill

      errors.add(:backfill_class_name, "must inherit from DataDrip::Backfill")
    end

    def validate_scope
      return unless backfill_class_name.present?
      return unless backfill_class

      begin
        backfill =
          backfill_class.new(
            batch_size: batch_size || 100,
            sleep_time: 5,
            backfill_options: options || {}
          )
        scope = backfill.scope

        scope =
          scope.limit(amount_of_elements) if amount_of_elements.present? &&
          amount_of_elements.positive?

        final_count = scope.count
        return unless final_count.zero?

        errors.add(
          :base,
          "No records to process with the current configuration. Please adjust your options or select a different backfill class."
        )
      rescue ActiveModel::UnknownAttributeError => e
        errors.add(:options, "contains unknown attributes: #{e.message}")
      end
    end

    def start_at_must_be_valid_datetime
      DateTime.parse(start_at.to_s)
    rescue ArgumentError, TypeError
      errors.add(:start_at, "must be a valid datetime")
    end
  end
end
