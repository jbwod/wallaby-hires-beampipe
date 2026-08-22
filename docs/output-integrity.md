# Output integrity and publication

There are two related JSON contracts:

1. the Wallaby package creates and verifies a local **output inventory**; and
2. a trusted publisher extends that inventory with durable-publication evidence
   and submits the resulting **Core verification report**.

`publish-local` does not submit to Core and does not manufacture an acknowledgement.

## Wallaby output inventory

`wallaby_hires.verify_output_products` is a DALiuGE-compatible function for the
final graph boundary. The defaults require at least one match for each pattern:

```text
**/image.*.10arc.final_mosaic.fits
**/weights.*.10arc.final_mosaic.fits
```

Every match must be a non-empty regular file below the output root, without a
symbolic-link path. The function records its relative path, byte size, and lowercase
SHA-256, then atomically writes `wallaby-output-inventory.json`.

```json
{
  "schema": "wallaby-hires-output-inventory/v1",
  "patterns": [
    "**/image.*.10arc.final_mosaic.fits",
    "**/weights.*.10arc.final_mosaic.fits"
  ],
  "pattern_counts": {
    "**/image.*.10arc.final_mosaic.fits": 1,
    "**/weights.*.10arc.final_mosaic.fits": 1
  },
  "products": [
    {
      "path": "HIPASSJ1318-21/image.HIPASSJ1318-21.10arc.final_mosaic.fits",
      "bytes": 1234,
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    },
    {
      "path": "HIPASSJ1318-21/weights.HIPASSJ1318-21.10arc.final_mosaic.fits",
      "bytes": 567,
      "sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    }
  ],
  "inventory_sha256": "0985ee8d5d0b59fbb48e51af0a033914560fcadcc6d4ad9dbb0443f182752d8d"
}
```

`inventory_sha256` is SHA-256 over compact JSON for the `products` array with
object keys sorted. The package emits products in relative-path order. It is not a
hash of the pretty-printed inventory file. The digest does not cover `patterns` or
`pattern_counts`, and inventory verification does not rediscover unlisted files;
the trusted publisher must preserve the complete inventory and expected policy.

Use the same runtime interpreter for command-line checks:

```bash
/daliuge/.venv/bin/python -m wallaby_hires inventory-outputs \
  /path/to/session-output
/daliuge/.venv/bin/python -m wallaby_hires verify-inventory \
  /path/to/session-output \
  /path/to/session-output/wallaby-output-inventory.json
```

Custom `--pattern` values replace the defaults and should be used only when the
project's pinned output policy expects those product classes.

## Publication

For a durable POSIX mount, publication verifies the source inventory, copies each
product through a temporary file, validates the copied SHA-256, atomically replaces
the destination file, writes the inventory, and verifies it again:

```bash
/daliuge/.venv/bin/python -m wallaby_hires publish-local \
  /path/to/session-output \
  /path/to/session-output/wallaby-output-inventory.json \
  /durable/wallaby/HIPASSJ1318-21
```

The destination must be a deployment-approved durable filesystem. The package does
not choose an object-store endpoint, bucket, credentials, retention policy,
overwrite policy, or publisher identity. An S3/Acacia publisher must implement the
same re-hash-before-acknowledgement guarantee independently.

## Beampipe Core verification report

For a production project configured with:

```yaml
output_verification:
  required: true
  inventory_schema: wallaby-hires-output-inventory/v1
```

DALiuGE/scheduler completion leaves output verification pending. A trusted,
authenticated publisher submits the complete Wallaby inventory plus these fields
to `POST /api/v2/executions/{id}/outputs/verify`:

```json
{
  "schema": "wallaby-hires-output-inventory/v1",
  "patterns": [
    "**/image.*.10arc.final_mosaic.fits",
    "**/weights.*.10arc.final_mosaic.fits"
  ],
  "pattern_counts": {
    "**/image.*.10arc.final_mosaic.fits": 1,
    "**/weights.*.10arc.final_mosaic.fits": 1
  },
  "products": [
    {
      "path": "HIPASSJ1318-21/image.HIPASSJ1318-21.10arc.final_mosaic.fits",
      "bytes": 1234,
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    },
    {
      "path": "HIPASSJ1318-21/weights.HIPASSJ1318-21.10arc.final_mosaic.fits",
      "bytes": 567,
      "sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    }
  ],
  "inventory_sha256": "0985ee8d5d0b59fbb48e51af0a033914560fcadcc6d4ad9dbb0443f182752d8d",
  "durable_destination_uri": "file:///durable/wallaby/run-01",
  "publication": {
    "acknowledged": true,
    "publisher": "wallaby-publisher",
    "receipt_id": "publication-01",
    "published_at": "2026-08-22T00:00:00Z"
  }
}
```

Core requires the schema pinned on the execution, non-empty unique safe relative
product paths, positive byte sizes, lowercase SHA-256 values, the canonical
`inventory_sha256`, a durable `s3`, `gs`, `https`, or absolute `file` URI, and a
positive authenticated publication acknowledgement. `patterns` and
`pattern_counts` remain part of the full Wallaby v1 report.

Core stores the full report as an immutable `output_inventory` artifact. The
artifact's own SHA-256 covers canonical JSON for the **whole report** and therefore
is expected to differ from `inventory_sha256`, which covers only `products`.

Core validates report shape and acknowledgement but cannot read the destination.
The trusted publisher is responsible for re-hashing durable objects before it
acknowledges publication and must protect its superuser credential.

<figure class="bp-diagram" aria-labelledby="output-trust-flow-title">
  <figcaption id="output-trust-flow-title">
    <strong>Required production target state; not currently graph-wired or
    completed live.</strong> Core accepts a publication report only after the
    trusted publisher has re-hashed the durable destination.
  </figcaption>
  <ol class="bp-flow bp-flow--publication">
    <li>
      <span class="bp-flow__eyebrow">DALiuGE runtime</span>
      <strong>Real ASKAPsoft mosaic</strong>
      <span>Non-empty image and weights</span>
      <small>Scheduler success alone is insufficient</small>
    </li>
    <li class="bp-flow__gap">
      <span class="bp-flow__eyebrow">Required graph integration</span>
      <strong>Wallaby verifier</strong>
      <span>Paths, sizes, product SHA-256 values</span>
      <small><code>inventory_sha256</code> covers canonical products only</small>
    </li>
    <li class="bp-flow__publisher">
      <span class="bp-flow__eyebrow">Trusted publisher boundary</span>
      <strong>Verify, publish, then read back</strong>
      <span>Validate the source inventory before copying</span>
      <div class="bp-durable-store">
        <span class="bp-durable-store__arrow" aria-hidden="true">⇅</span>
        <strong>Approved durable store</strong>
        <small>Atomic copy, destination re-hash, publication receipt</small>
      </div>
      <small>Add the durable URI and acknowledge only after hashes match</small>
    </li>
    <li>
      <span class="bp-flow__eyebrow">Beampipe Core</span>
      <strong>Authenticated verify report</strong>
      <span>Validate schema, auth, digest, URI, acknowledgement</span>
      <small>Artifact SHA-256 covers the whole report; resolve output state</small>
    </li>
  </ol>
  <p class="bp-stop-branch">
    <strong>Stop branch:</strong> no-download stubs produce zero-byte placeholders
    → not output evidence → do not publish or submit a Core verification report.
  </p>
  <p class="bp-diagram__note">
    Core does not read the durable store. The authenticated publisher's verified
    acknowledgement is the storage trust root. The mosaic-to-verifier connection
    shown here is required production wiring and is not yet present in the graph.
  </p>
</figure>

## Current graph gap

The current EAGLE production graphs do not yet place
`verify_output_products` downstream of the real ASKAPsoft mosaic node. The
no-download graph intentionally creates zero-byte placeholders and sets output
verification to `required: false`; it cannot be used as output evidence.

Before Beampipe treats a production execution as successful, deployment must:

1. invoke the verifier only after a successful real mosaic;
2. persist the local inventory in the execution's evidence boundary;
3. publish to durable storage and re-hash the destination;
4. construct and submit the authenticated Core report; and
5. verify that Core records the `output_inventory` artifact and terminal output
   state.

Until that wiring exists and has completed live, scheduler/DALiuGE completion is
not proof that durable science products exist.
