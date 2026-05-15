# frozen_string_literal: true

module DataDrip
  module BackfillRunsHelper
    include ActionView::Helpers::NumberHelper

    def status_tag(status)
      status_str = status.to_s.downcase
      badge_class = "ap-badge ap-badge-#{status_str}"
      content_tag(:span, status_str.capitalize, class: badge_class)
    end

    def format_datetime_in_user_timezone(datetime, user_timezone = "UTC")
      return "" unless datetime

      user_timezone = user_timezone.presence || "UTC"
      local_time = datetime.in_time_zone(user_timezone)

      local_time.strftime("%b %d, %H:%M")
    end

    def format_insight_duration(seconds)
      return "—" if seconds.nil? || !seconds.finite? || seconds.negative?

      secs = seconds.to_f
      if secs < 60
        "#{secs.round(1)} s"
      elsif secs < 3600
        mins = (secs / 60).floor
        rem = (secs % 60).round
        "#{mins}m #{rem}s"
      else
        hours = (secs / 3600).floor
        mins = ((secs % 3600) / 60).floor
        "#{hours}h #{mins}m"
      end
    end

    def format_insight_elements_per_second(rate)
      return "—" if rate.nil? || !rate.finite? || rate.negative?

      "#{number_with_delimiter(rate.round(2))} /s"
    end

    def backfill_option_inputs(backfill_run)
      return "" unless backfill_run.backfill_class&.backfill_options_class

      attribute_types =
        backfill_run.backfill_class.backfill_options_class.attribute_types
      return "" if attribute_types.empty?

      content_tag :div, class: "ap-options-section" do
        header_content =
          content_tag :h3, "Options", class: "ap-options-title"

        inputs_content =
          attribute_types
            .map do |name, type|
              content_tag :div, class: "ap-field" do
                label_content =
                  label_tag "backfill_run[options][#{name}]",
                            name.to_s.humanize,
                            class: "ap-field-label"

                input_content =
                  case type
                  when ActiveModel::Type::String,
                       ActiveModel::Type::ImmutableString
                    text_field_tag "backfill_run[options][#{name}]",
                                   backfill_run.options[name],
                                   class: "ap-field-input"
                  when ActiveModel::Type::Integer, ActiveModel::Type::BigInteger
                    number_field_tag "backfill_run[options][#{name}]",
                                     backfill_run.options[name],
                                     class: "ap-field-input",
                                     step: 1
                  when ActiveModel::Type::Decimal, ActiveModel::Type::Float
                    number_field_tag "backfill_run[options][#{name}]",
                                     backfill_run.options[name],
                                     class: "ap-field-input",
                                     step: 0.01
                  when ActiveModel::Type::Boolean
                    content_tag :div, style: "display:flex;align-items:center;gap:8px;" do
                      check_box_tag(
                        "backfill_run[options][#{name}]",
                        "1",
                        backfill_run.options[name]
                      ) +
                        label_tag(
                          "backfill_run[options][#{name}]",
                          "Yes",
                          style: "font-size:13.5px;color:#374151;"
                        )
                    end
                  when ActiveModel::Type::Date
                    date_field_tag "backfill_run[options][#{name}]",
                                   backfill_run.options[name],
                                   class: "ap-field-input"
                  when ActiveModel::Type::Time
                    time_field_tag "backfill_run[options][#{name}]",
                                   backfill_run.options[name],
                                   class: "ap-field-input"
                  when ActiveModel::Type::DateTime
                    datetime_field_tag "backfill_run[options][#{name}]",
                                       backfill_run.options[name],
                                       class: "ap-field-input"
                  else
                    text_area_tag "backfill_run[options][#{name}]",
                                  backfill_run.options[name],
                                  class: "ap-field-input",
                                  rows: 3
                  end

                label_content + input_content
              end
            end
            .join
            .html_safe

        header_content + inputs_content
      end
    end
  end
end
