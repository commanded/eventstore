SELECT
	source_uuid,
	source_version,
	source_type,
	convert_from(data, current_setting('server_encoding')) as data,
	convert_from(metadata, current_setting('server_encoding')) as metadata,
	created_at
FROM snapshots
ORDER BY created_at;
