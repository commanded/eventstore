defmodule EventStore.Subscriptions.AllStreamsSubscription do
  @moduledoc false
  @behaviour EventStore.Subscriptions.StreamSubscriptionProvider

  alias EventStore.{RecordedEvent,Storage}
  alias EventStore.Streams.AllStream

  @all_stream "$all"

  def extract_ack({event_number, _stream_version}),
    do: event_number

  def event_number(%RecordedEvent{event_number: event_number}),
    do: event_number

  def last_ack(%Storage.Subscription{last_seen_event_number: last_seen_event_number}),
    do: last_seen_event_number

  def unseen_event_stream(@all_stream, last_seen, read_batch_size),
    do: AllStream.stream_forward(last_seen + 1, read_batch_size)

  def ack_last_seen_event(@all_stream, subscription_name, last_event_number),
    do: Storage.ack_last_seen_event(@all_stream, subscription_name, last_event_number, nil)
end
