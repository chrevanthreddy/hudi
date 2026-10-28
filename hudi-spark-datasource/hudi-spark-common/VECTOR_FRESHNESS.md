# Vector index audit diagnostics

## Limitations and writer compatibility

Tables with a vector index are not writable by older binaries that do not recognize the vector
metadata partition. Such writers fail at metadata-partition dispatch in
`MetadataPartitionType.fromPartitionPath`, the existing behavior for any unrecognized MDT partition;
no new fencing mechanism is introduced. Same-version writers maintain the index unconditionally,
including clients initialized before index creation. A production-client regression verifies that
case. Release table-version confirmation is documentation context, not a dependency of this boundary.

Markers are per-commit audit records. Strict validation is a read-only, opt-in diagnostic and has
no correctness role in the supported writer protocol. It neither replaces writer maintenance nor
candidate liveness arbitration. No supported missing-hook writer scenario has been established.

## Diagnostic options

Normal writes maintain vector postings and emit one source-instant marker per index generation
and commit. They do not advance a manifest coverage frontier. RLI arbitration remains mandatory
for candidate liveness; it is not a guarantee of ranking completeness after an unindexed write.

Freshness validation is opt-in: `vector.freshness.strict=true`. The default is `false`, which
skips source-marker validation. With strict validation enabled, completed source writes after
the immutable bootstrap baseline must have markers in the active generation. Pending writes and
non-write timeline actions are not treated as missing markers. Archived completed history is
included when the bootstrap baseline predates the active timeline.

Each query option resolves independently: runtime option, then persisted index option, then its
default. Changing `vector.query.mode` does not change `vector.freshness.policy`.
The policy defaults to `WARN` for both approximate and exact-rerank queries:

- `FAIL`: reject the query and identify the first unmarked completed write.
- `WARN`: log the coverage gap and continue indexed search; ranking freshness is not guaranteed.
- `FALLBACK`: use brute-force exact search instead of the vector index.

The policy is consulted only with strict validation enabled. A manifest missing its bootstrap
baseline is invalid and rejected at metadata-cache load, regardless of freshness policy.

**Repair a detected gap by dropping and recreating the vector index.** Catch-up/replay repair
is not implemented. Recreating the index must use the normal index-build coordination flow.

Archived source-write history is cached per table and replaced when the bootstrap baseline or
active-timeline start changes. Active writes and marker validation outcomes are never cached.
Strict validation still scans the generation's markers and may be expensive for long histories;
it is not part of the default query path.

The optional diagnostic does not define writer compatibility or a freshness protocol.
