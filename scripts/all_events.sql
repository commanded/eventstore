SELECT
  se.stream_version as event_number,
  e.event_id,
  s.stream_uuid,
  se.original_stream_version as stream_version,
  e.event_type,
  e.correlation_id,
  e.causation_id,
  convert_from(e.data, current_setting('server_encoding')) as data,
  convert_from(e.metadata, current_setting('server_encoding')) as metadata,
  e.created_at
FROM stream_events se
INNER JOIN streams s ON s.stream_id = se.original_stream_id
INNER JOIN events e ON se.event_id = e.event_id
WHERE se.stream_id = 0
ORDER BY se.stream_version ASC
LIMIT 10;
