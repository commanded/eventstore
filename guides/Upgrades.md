# Upgrading an EventStore

The [CHANGELOG](https://github.com/slashdotdash/eventstore/blob/master/CHANGELOG.md) is used to indicate when a schema migration is required for a given version of the EventStore.

You can upgrade an existing EventStore database using the following mix task:

```console
$ mix event_store.migrate
```

Run this command each time you upgrade; it is safe to run multiple times.

Creating an EventStore, using the `mix event_store.create` task, will always use the latest database schema.
