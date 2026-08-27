# Output integrity and publication

The Setonix production graph finishes with the project-neutral
`beampipe-publish` DALiuGE application from the separately installed
`beampipe-pallette` package. Publication is part of the graph, not an operator
step after DALiuGE.

The Wallaby graph owns only its output policy:

```text
**/image.*.10arc.final_mosaic.fits
**/weights.*.10arc.final_mosaic.fits
```

Both patterns must match at least one non-empty regular file under
`BEAMPIPE_OUTPUT_ROOT`. The publisher independently scans the output tree,
hashes the selected files, publishes them to the configured durable destination,
reads every durable object back, and requires the size and SHA-256 to match.

## Terminal graph contract

The final mosaic application has a dedicated `publication_ready` path output.
Its command creates that sentinel only after `linmos` exits successfully. The
sentinel FileDROP is the publisher's single required completion input, so a
missing or failed mosaic cannot trigger publication.

The publisher is the native `beampipe_pallette.apps.BeampipePublishApp`
barrier application. In EAGLE it exposes only the completion input, inventory
output, and Wallaby-owned pattern policy; storage, execution identity, Core, and
handoff settings cannot be edited into the graph. It writes and fsyncs a
session-scoped `beampipe-output-inventory.json` FileDROP, then atomically writes
the byte-identical receipt to the control-plane handoff path.
The emitted document and the durable report use:

```text
beampipe-output-inventory/v1
```

Each product records its safe relative path, positive byte count, and lowercase
SHA-256. The inventory also records the exact project patterns and their counts.
Retries are idempotent: an existing byte-identical durable object is reused, but
an object at the same destination key with different bytes fails closed.

## Runtime values

Dynamic values are deliberately absent from the graph. Beampipe supplies these
to the DALiuGE runtime for each execution:

| Variable | Meaning |
| --- | --- |
| `BEAMPIPE_OUTPUT_ROOT` | Completed Wallaby output tree to scan. |
| `BEAMPIPE_OUTPUT_DESTINATION_URI` | Approved `file://` or `s3://` base destination. |
| `BEAMPIPE_EXECUTION_ID` | Execution UUID. |
| `BEAMPIPE_EXECUTION_ATTEMPT` | Literal zero-based Core retry count. |
| `BEAMPIPE_OUTPUT_INVENTORY_HANDOFF_PATH` | Required absolute receipt path prepared in the retained session directory and outside the output tree. |

The publisher always creates an execution-attempt namespace:

```text
<destination-base>/executions/<execution-uuid>/attempt-<retry-count>
```

Install `beampipe-pallette` with the same interpreter used by every DALiuGE
executor. When `s3://` is selected, install its S3 extra:

```bash
/daliuge/.venv/bin/python -m pip install 'beampipe-pallette[s3]==0.4.0'
/daliuge/.venv/bin/beampipe-publish --version
```

Filesystem/project storage requires an absolute, non-root URI and rejects source
and destination overlap. S3-compatible storage uses conditional create and full
read-back verification, including multipart upload for products above 5 GiB.
NGAS is not implemented by this release; selecting an unsupported URI fails
before publication.

## Control-plane boundary

The publisher never receives a Core URL or credential. The transport constructs
the deterministic handoff path under the retained remote session directory,
creates its parent before DALiuGE starts, and supplies only that path to the
runtime. The publisher requires an absolute, non-root, non-symlink path outside
`BEAMPIPE_OUTPUT_ROOT`; it creates the receipt file atomically, reads it back,
and fsyncs both file and parent directory. Existing byte-identical evidence is
reusable, while conflicting bytes fail closed.

Each retry receives its own immutable path:

```text
<remote-session-dir>/.beampipe/publication/attempt-<retry-count>/beampipe-output-inventory.json
```

## Core receipt ingestion

The terminal component does not call Core. After remote execution, the transport
reads the canonical handoff bytes and submits them through Core's trusted
control-plane path. The document binds the evidence to:

- top-level canonical `execution_id`;
- top-level zero-based `execution_attempt`;
- the exact output patterns, counts, product paths, sizes, and SHA-256 values;
- the durable destination URI and inventory digest; and
- deterministic publisher receipt identity.

Core stores the complete canonical receipt as an immutable output-inventory
artifact. DALiuGE or scheduler completion alone remains insufficient when output
verification is required; Core reaches terminal success only after both backend
success and this transported, verified receipt are present.

## Failure and retry rules

- No match, an empty product, a symlink, an unsafe path, or a source mutation
  stops before handoff.
- A durable read-back mismatch stops the run and does not report verification.
- A handoff persistence or read-back failure fails the DALiuGE terminal app.
- A different receipt at the same execution-attempt handoff path is rejected.
- The no-download graph exercises the same terminal contract with explicitly
  synthetic placeholder outputs; those are qualification evidence only.

The standalone package README defines the generic glob grammar, storage adapter,
receipt-size, and handoff rules. This document records only the Wallaby-specific
patterns and production graph wiring.
