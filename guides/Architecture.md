### PostgreSQL usage

EventStore creates multiple connections to the Postres database:

- a pooled connection, that you configure via `config/config.exs`, used for most database operations (e.g. reading/appending events);
- a connection used to listen for event notifications (using Postgres' `LISTEN` / `NOTIFY`);
- and another connection for subscription [advisory locks](https://www.postgresql.org/docs/current/static/explicit-locking.html#ADVISORY-LOCKS).

If you configure EventStore to use a `pool_size` of 10, then you will have 12 Postgres database connections in total.

The pooled connection uses `:exp` back-off. However the other connections use `:stop` back-off so that the connection process terminates when the database connection is broken. These connections are monitored by another process and will be restarted.

#### Why are these connections stopped on error?

Related processes need to be notified when the connection exits or is restarted. The [postgrex](https://hexdocs.pm/postgrex/) library doesn't provide a way of being notified when the connection terminates. As an example, EventStore uses [Postgres' advisory locks](https://www.postgresql.org/docs/current/static/explicit-locking.html#ADVISORY-LOCKS) to guarantee only one instance of a subscription runs, regardless of how many nodes are running (or whether they are clustered). These advisory locks are tied to a database connection and are released when the connection terminates. When this happens, EventStore attempts to reacquire the locks. The `EventStore.MonitoredServer` module provides this process monitoring and `after_exit` and `after_restart` callback functions.
