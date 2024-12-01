## Frequently asked questions

- [What version of PostgreSQL is supported?](#what-version-of-postgresql-is-supported)
- [Which PostgreSQL hosting provider is supported?](#which-postgresql-hosting-provider-is-supported)

---

### What version of PostgreSQL is supported?

PostgreSQL v9.5 or later.

---

### Which PostgreSQL hosting provider is supported?

You can verify a potential hosting provider by running the EventStore test suite against an instance of the hosted PostgreSQL database. If all tests pass, then you should be fine.

To run the test suite you must first clone this GitHub repository:

```console
git clone https://github.com/commanded/eventstore.git
cd eventstore
mix deps.get
```

Then configure your database connection settings for the test environment, in `config/test.exs`.

Once configured, you can create the test database and run the tests:

```console
MIX_ENV=test mix do event_store.create, event_store.init
mix test
```

