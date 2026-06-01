# CDM last-write-wins with ZDM Proxy

You can use CDM alone, with [ZDM Proxy](https://github.com/datastax/zdm-proxy), or for data validation after using another data migration tool.

When using CDM with ZDM Proxy, Cassandra's last-write-wins semantics ensure that new, real-time writes accurately take precedence over historical writes.

Last-write-wins compares the `writetime` of conflicting records, and then retains the most recent write.

For example, if a new write occurs in your target cluster with a `writetime` of `2023-10-01T12:05:00Z`, and then CDM migrates a record against the same row with a `writetime` of `2023-10-01T12:00:00Z`, the target cluster retains the data from the new write because it has the most recent `writetime`.