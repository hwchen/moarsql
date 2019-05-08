# moarSQL

Clickhouse allows only one join per select, so multiple joins means lots of subqueries.

This quick utility rewrites a multi-join into clickhouse sql
