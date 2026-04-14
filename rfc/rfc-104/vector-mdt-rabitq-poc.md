## MDT RaBitQ POC

This POC adds an alternate storage path for RaBitQ codes so the same vector index can be inspected in two ways:

- `vector.rabitq.storage=hidden_columns`
  Keeps the existing design where RaBitQ codes live in base-table hidden columns.
- `vector.rabitq.storage=mdt_lookup`
  Writes RaBitQ codes into metadata-table posting rows instead of relying on base-file hidden columns.
- `vector.rabitq.storage=both`
  Writes the MDT posting rows in addition to the hidden-column path so notebook experiments can compare both layouts on the same index build.

The default remains `hidden_columns`.

## What Gets Written

The existing vector index metadata rows are still produced:

- `__centroids__`
- `__quantizer__`
- `__fg__/<cluster_id>/<partition_path>`
- assignment rows keyed by record key

When `vector.rabitq.storage` is `mdt_lookup` or `both`, two extra row shapes are emitted inside the same `vector_index_<name>` MDT partition:

- `__manifest__`
  Stores the active posting generation for the index build.
- `__posting__/<generation_id>/<cluster_id>/<record_key>`
  Stores the RaBitQ binary code and optional scalar for one vector.

This keeps the POC compact: it reuses the existing vector MDT partition instead of introducing a second metadata partition family before the design is finalized.

## Example

```sql
CREATE INDEX embedding_idx
ON products
USING VECTOR (embedding)
OPTIONS (
  'vector.dimension' = '768',
  'vector.algorithm' = 'ivfflat',
  'vector.quantizer' = 'IVF_RABITQ',
  'vector.num_clusters' = '256',
  'vector.rabitq.storage' = 'both'
);
```

After bootstrap, `hudi_metadata(...)` can show:

- one `__manifest__` row with the active generation id
- many `__posting__/...` rows containing `binaryCode` and `scalar`
- the existing centroid, quantizer, assignment, and `fg_mapping` rows

## How It Works

During bootstrap:

1. Hudi trains IVF centroids exactly as before.
2. It assigns each record to a cluster exactly as before.
3. If the quantizer is `IVF_RABITQ` and `vector.rabitq.storage` includes MDT storage, Hudi encodes each feature vector with `RaBitQEncoder`.
4. Hudi writes one manifest row for the build generation and one posting row per indexed record.

Each posting row carries:

- `entryType=POSTING`
- `generationId=<build timestamp>`
- `clusterId=<assigned cluster>`
- `fileGroupId=<source file group>`
- `partitionPath=<data partition>`
- `binaryCode=<packed RaBitQ bits>`
- `scalar=<norm factor when vectors are not assumed normalized>`

The manifest row carries:

- `entryType=MANIFEST`
- `generationId=<active generation>`
- quantizer metadata copied from the build

## Why This Helps

The MDT-native path gives the POC a way to compare:

- hidden-column materialization in base files
- MDT-only code storage that avoids schema augmentation on the base table

That is especially useful for the current investigation because hidden-column persistence has been the fragile part of the flow.

## Current Scope

This patch is intentionally limited to write-side metadata generation and metadata visibility.

- Existing pruning still uses centroids plus `fg_mapping`.
- Existing query-time approximate ranking is not yet switched over to read `__posting__` rows.

For hidden-column experiments, the cluster smoke/benchmark script now includes a distributed
query flow (`scripts/spark_connect_vector_gcs_test.py`) that runs:

1. MDT coarse pruning via vector read options
2. RaBitQ approximate scoring from `_hudi_vec_*` hidden columns
3. exact rerank on a bounded shortlist

Tuning knobs:

- `HOODIE_VECTOR_QUERY_NPROBES`
- `HOODIE_VECTOR_QUERY_REFINE_FACTOR`
- `HOODIE_VECTOR_QUERY_TOPK`
- `HOODIE_VECTOR_ENABLE_FULL_SCAN_BASELINE` (disable this for very large runs)

That keeps the comparison simple: the notebook can validate that the alternate MDT-native payloads are present before a larger query-path change is attempted.
