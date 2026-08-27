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
credential settings cannot be edited into the graph. It writes and fsyncs a
session-scoped `beampipe-output-inventory.json` FileDROP before it reports the
receipt to Core.
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
| `BEAMPIPE_CORE_URL` | HTTPS Core origin or `/api/v2` base. |
| `BEAMPIPE_EXECUTION_ID` | Execution UUID. |
| `BEAMPIPE_EXECUTION_ATTEMPT` | Literal zero-based Core retry count. |
| `BEAMPIPE_PUBLISHER_TOKEN_FILE` | Absolute path to the execution-scoped mode-0600 token. |

The publisher always creates an execution-attempt namespace:

```text
<destination-base>/executions/<execution-uuid>/attempt-<retry-count>
```

Install `beampipe-pallette` with the same interpreter used by every DALiuGE
executor. When `s3://` is selected, install its S3 extra:

```bash
/daliuge/.venv/bin/python -m pip install 'beampipe-pallette[s3]==0.2.0'
/daliuge/.venv/bin/beampipe-publish --version
```

Filesystem/project storage requires an absolute, non-root URI and rejects source
and destination overlap. S3-compatible storage uses conditional create and full
read-back verification, including multipart upload for products above 5 GiB.
NGAS is not implemented by this release; selecting an unsupported URI fails
before publication.

## Publisher credential boundary

Core issues a short-lived credential restricted to the current execution,
attempt, and `verify_outputs` action. The graph never receives a Core superuser
credential. Inline bearer tokens are disabled in the production graph, Core
callbacks require HTTPS, redirects are refused, and there is no TLS-disable
option.

The private token file is deliberately retained after a valid acknowledgement
so DALiuGE can replay the exact immutable receipt if a late DROP/session failure
occurs. Core accepts only the byte-identical receipt for that execution attempt
and revokes the credential during terminal reconciliation or expiry. Normal
session/operator retention cleanup removes the retained file; outer-job EXIT
unlinking is future runtime hardening.

## Core acknowledgement

The terminal component posts the publication receipt to:

```text
POST /api/v2/executions/<execution-uuid>/outputs/verify
```

It accepts success only when the response binds all of these values back to the
request:

- execution UUID and retry count;
- `output_state=verified`;
- artifact kind `output_inventory`;
- exact canonical report SHA-256 and durable URI; and
- artifact execution attempt.

Core stores the complete canonical receipt as an immutable output-inventory
artifact. DALiuGE or scheduler completion alone remains insufficient when output
verification is required; Core reaches terminal success only after both backend
success and this verified receipt are present.

## Failure and retry rules

- No match, an empty product, a symlink, an unsafe path, or a source mutation
  stops before acknowledgement.
- A durable read-back mismatch stops the run and does not report verification.
- A callback timeout or lost response retains the token and inventory FileDROP so
  the same receipt can be replayed.
- A different receipt for an already consumed execution-attempt credential is
  rejected.
- The no-download graph creates test placeholders and cannot provide production
  output evidence.

The standalone package README defines the generic glob grammar, storage adapter,
receipt-size, HTTP, and token-file rules. This document records only the
Wallaby-specific patterns and production graph wiring.
