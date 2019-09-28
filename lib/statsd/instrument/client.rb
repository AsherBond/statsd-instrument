# frozen_string_literal: true

require 'statsd/instrument/datagram'
require 'statsd/instrument/datagram_builder'
require 'statsd/instrument/statsd_datagram_builder'
require 'statsd/instrument/dogstatsd_datagram_builder'
require 'statsd/instrument/null_sink'
require 'statsd/instrument/udp_sink'
require 'statsd/instrument/capture_sink'
require 'statsd/instrument/log_sink'

# The Client is the main interface for using StatsD.
class StatsD::Instrument::Client
  attr_reader :sink, :datagram_builder_class, :prefix, :default_tags, :default_sample_rate

  def initialize(
    sink: StatsD::Instrument::NullSink.new,
    prefix: nil,
    default_sample_rate: 1,
    default_tags: nil,
    datagram_builder_class: StatsD::Instrument::Environment.datagram_builder_class
  )
    @sink = sink
    @datagram_builder_class = datagram_builder_class

    @prefix = prefix
    @default_tags = default_tags
    @default_sample_rate = default_sample_rate
  end

  # @!group Metric Methods

  # Emits a counter metric.
  #
  # You should use a counter metric to count the frequency of something happening. As a
  # result, the value should generally be set to 1 (the default), unless you reporting
  # about a batch of activity. E.g. `increment('messages.processed', messages.size)`
  # For values that are not frequencies, you should use another metric type, e.g.
  # {#histogram} or {#distribution}.
  #
  # @param name [String] The name of the metric.
  #
  #   - We recommend using `snake_case.metric_names` as naming scheme.
  #   - A `.` should be used for namespacing, e.g. `foo.bar.baz`
  #   - A metric name should not include the following characters: `|`, `@`, and `:`.
  #     The library will convert these characters to `_`.
  #
  # @param value [Integer] (default: 1) The value to increment the counter by.
  #
  #   You should not compensate for the sample rate using the counter increment. E.g., if
  #   your sample rate is set to `0.01`, you should not use 100 as increment to compensate
  #   for it. The sample rate is part of the packet that is being sent to the server, and
  #   the server should know how to compensate for it.
  #
  # @param [Float] sample_rate (default: `#default_sample_rate`) The rate at which to sample
  #   this metric call. This value should be between 0 and 1. This value can be used to reduce
  #   the amount of network I/O (and CPU cycles) is being used for very frequent metrics.
  #
  #   - A value of `0.1` means that only 1 out of 10 calls will be emitted; the other 9 will
  #     be short-circuited.
  #   - When set to `1`, every metric will be emitted.
  #   - If this parameter is not set, the default sample rate for this client will be used.
  #
  # @param [Hash<Symbol, String>, Array<String>] tags (default: nil)
  # @return [void]
  def increment(name, value = 1, sample_rate: nil, tags: nil)
    sample_rate ||= @default_sample_rate
    return unless sample?(sample_rate)
    emit(datagram_builder.c(name, value, sample_rate, tags))
  end

  # Emits a timing metric.
  #
  # @param name (see #increment)
  # @param [Numeric] value The duration to record, in milliseconds.
  # @param sample_rate (see #increment)
  # @param tags (see #increment)
  # @return [void]
  def measure(name, value = nil, sample_rate: nil, tags: nil)
    sample_rate ||= @default_sample_rate
    return unless sample?(sample_rate)
    emit(datagram_builder.ms(name, value, sample_rate, tags))
  end

  # Emits a gauge metric.
  #
  # You should use a gauge if you are reporting the current value of
  # something that can only have one value at the time. E.g., the
  # speed of your car. A newly reported value will repla e the previously
  # reported value.
  #
  #
  # @param name (see #increment)
  # @param [Numeric] value The gauged value.
  # @param sample_rate (see #increment)
  # @param tags (see #increment)
  # @return [void]
  def gauge(name, value, sample_rate: nil, tags: nil)
    sample_rate ||= @default_sample_rate
    return unless sample?(sample_rate)
    emit(datagram_builder.g(name, value, sample_rate, tags))
  end

  # Emits a set metric, which counts distinct values.
  #
  # @param name (see #increment)
  # @param [Numeric, String] value The value to count for distinct occurrences.
  # @param sample_rate (see #increment)
  # @param tags (see #increment)
  # @return [void]
  def set(name, value, sample_rate: nil, tags: nil)
    sample_rate ||= @default_sample_rate
    return unless sample?(sample_rate)
    emit(datagram_builder.s(name, value, sample_rate, tags))
  end

  # Emits a distribution metric, which builds a histogram of the reported
  # values.
  #
  # @note The distribution metric type is not available on all implementations.
  #   A `NotImplemetedError` will be raised if you call this method, but
  #   the active implementation does not support it.
  #
  # @param name (see #increment)
  # @param [Numeric] value The value to include in the distribution histogram.
  # @param sample_rate (see #increment)
  # @param tags (see #increment)
  # @return [void]
  def distribution(name, value, sample_rate: nil, tags: nil)
    sample_rate ||= @default_sample_rate
    return unless sample?(sample_rate)
    emit(datagram_builder.d(name, value, sample_rate, tags))
  end

  # Emits a histogram metric, which builds a histogram of the reported values.
  #
  # @note The histogram metric type is not available on all implementations.
  #   A `NotImplemetedError` will be raised if you call this method, but
  #   the active implementation does not support it.
  #
  # @param name (see #increment)
  # @param [Numeric] value The value to include in the histogram.
  # @param sample_rate (see #increment)
  # @param tags (see #increment)
  # @return [void]
  def histogram(name, value, sample_rate: nil, tags: nil)
    sample_rate ||= @default_sample_rate
    return unless sample?(sample_rate)
    emit(datagram_builder.h(name, value, sample_rate, tags))
  end

  # @!endgroup

  # Instantiates a new StatsD client that uses the settings of the current client,
  # except for the provided overrides.
  #
  # @yield [client] A new client will be constructed with the altered settings, and
  #   yielded to the block. The original client will not be affected. The new client
  #   will be disposed after the block returns
  # @return The return value of the block will be passed on as return value.
  def with_options(
    sink: nil,
    prefix: nil,
    default_sample_rate: nil,
    default_tags: nil,
    datagram_builder_class: nil
  )
    client = clone_with_options(sink: sink, prefix: prefix,
      default_sample_rate: default_sample_rate, default_tags: default_tags,
      datagram_builder_class: datagram_builder_class)

    yield(client)
  end

  def clone_with_options(
    sink: nil,
    prefix: nil,
    default_sample_rate: nil,
    default_tags: nil,
    datagram_builder_class: nil
  )
    self.class.new(
      sink: sink || @sink,
      prefix: prefix || @prefix,
      default_sample_rate: default_sample_rate || @default_sample_rate,
      default_tags: default_tags || @default_tags,
      datagram_builder_class: datagram_builder_class || @datagram_builder_class,
    )
  end

  def capture_sink
    StatsD::Instrument::CaptureSink.new(parent: @sink)
  end

  def with_capture_sink(capture_sink)
    @sink = capture_sink
    yield
  ensure
    @sink = @sink.parent
  end

  # Captures metrics that were emitted during the provided block.
  #
  # @yield During the execution of the provided block, metrics will be captured.
  # @return [Array<StatsD::Instagram::Datagram>] The list of metrics that were
  #   emitted during the block, in the same order in which they were emitted.
  def capture(&block)
    sink = capture_sink
    with_capture_sink(sink, &block)
    sink.datagrams
  end

  protected

  def datagram_builder
    @datagram_builder ||= @datagram_builder_class.new(prefix: prefix, default_tags: default_tags)
  end

  def sample?(sample_rate)
    sample_rate == 1 || rand < sample_rate
  end

  def emit(datagram)
    @sink << datagram
    nil
  end
end