# InfluxDB 3 Go adapter

Bounded reads through InfluxDB 3's `/api/v3/query_sql` HTTP API. An application
maps each DALgo collection to one existing table, its unique non-null string
key column, and an explicit allowlist of scalar columns. Construction is lazy.
No database/table discovery, raw SQL, credentials in URLs, redirects, proxy
inheritance, or SQL fallback is provided.

```go
client, err := dalgo2influxdb.New("https://influx.example", dalgo2influxdb.Options{
    Database: "metrics",
    TokenProvider: func(ctx context.Context) (string, error) {
        return scopedReadToken(ctx)
    },
    Tables: map[string]dalgo2influxdb.Table{
        "events": {
            Name: "existing_events",
            Key: dalgo2influxdb.Column{Name: "event_id", Kind: "string"},
            Columns: map[string]dalgo2influxdb.Column{
                "value": {Name: "value", Kind: "number"},
            },
        },
    },
})
if err != nil { return err }
defer client.Close()
row, err := client.Get(ctx, "events", "event-123")
```

The mapping is trusted application configuration, not an authorization system.
Use an existing read-scoped credential and enforce collection access outside
the adapter. Do not expose database credentials in a browser. `Transport`, when
provided, must be a concrete verified-TLS `*http.Transport`; the adapter clones
it, disables proxies/compression/redirects, and owns only that clone. Custom
dialers and certificate callbacks are trusted caller configuration.

| Capability | Go adapter state |
| --- | --- |
| Get / Exists / GetMulti | Implemented for mapped string keys; batch order, duplicates, and missing rows retained |
| Native query | Implemented for mapped scalar comparisons joined by AND, projection, deterministic ordering, and bounded keyset pages |
| DALgo structured query | Partial: one collection, scalar AND filters, projection, ordering, limit; advanced expressions, joins, groups, aliases, offsets, and generic cursor translation are not implemented |
| Native values | Native rows and DALgo map/map-pointer targets retain JSON scalar numbers as `json.Number`; struct targets use DALgo's normal JSON mapping (declare numeric fields as `json.Number` for exact lexemes); no claim to restore precision absent from the server response |
| Append/write | Not implemented in this Go adapter; InfluxDB supports writes |
| Transactions / schema / indexes | Not implemented through this adapter |
| Installed DataTug CLI / browser / hosted account | Unverified; separate integration/proof gates |

`Query.StartAfter` and `Page.NextCursor` are native typed keyset values, not
DALgo opaque cursors. Non-null order columns and a final unique key tie breaker
are required. Each request fetches at most one lookahead row to establish another
page. Pages are independent reads, not a transaction or stable snapshot while
the underlying database changes. Generic `RecordsReader.Cursor` is deliberately
not implemented.

Bounds cover the entire operation, including token resolution, SQL planning,
all GetMany/GetMulti requests, parsing, record callbacks, and final delivery.
Caller-owned DALgo record callbacks run synchronously. Arbitrary blocking
callbacks cannot be safely preempted; the public call waits for them and checks
cancellation/deadline immediately afterward, with no later record mutation from
a detached worker. An unbounded generic DALgo query uses the configured row cap
and fails on proven overflow rather than silently returning the first page.

Defaults: 30 seconds, 1,000 rows, 4 MiB response bytes, 64 KiB request bytes,
and four admitted operations. Hard maxima: two minutes, 10,000 rows, 16 MiB
responses, 1 MiB requests, and 16 admitted operations. A query may charge one
additional lookahead row; no partial result escapes an exceeded bound.
Headers are limited to 64 KiB; responses must be uncompressed JSON arrays with
only the requested scalar columns. Duplicate members/keys, overreturns, missing
columns, invalid values, predicate violations, backward cursors, and incorrect
ordering fail the whole request. Limits do not bound allocations inside a
caller-supplied token provider, dialer, or certificate callback.

Caller cancellation, deadline, and `Close` cancel active HTTP requests and
prevent fresh delivery. An uncooperative token provider retains its admission
lease until it actually returns; repeated cancellation cannot create unlimited
detached work. Close is idempotent. Create a new client to reopen. Errors redact
HTTP response bodies, credentials, endpoint details, and token-provider errors.

Checks use owned local verified-TLS HTTP fixtures for the official API wire
shape, native numeric lexemes, paging, limits, refusal paths, and cancellation.
These are protocol fixtures, not a running InfluxDB service or Cloud account.
Real InfluxDB 3/Core/Enterprise/Cloud behavior, hosted authorization, installed
CLI behavior, and release availability require separate evidence. Current check
results and independent review are recorded by the owning delivery workflow;
source presence alone is not a release claim.

API references: [SQL endpoint](https://docs.influxdata.com/influxdb3/core/query-data/execute-queries/influxdb-v3-api/)
and [WHERE parameters](https://docs.influxdata.com/influxdb3/core/query-data/sql/parameterized-queries/).

The adapter requires Go 1.26+ and public DALgo v0.97.0; no local module replace
or additional core implementation is used.
