# Output integrity and publication

`wallaby_hires.verify_output_products` is a DALiuGE-compatible function for the
final graph boundary. It requires both the final image mosaic and its weights file
to exist as non-empty regular files, computes their SHA-256 hashes and byte sizes,
and writes `wallaby-output-inventory.json` atomically.

The same checks are available from the command line:

```bash
wallaby_hires inventory-outputs /path/to/session-output
wallaby_hires verify-inventory /path/to/session-output \
  /path/to/session-output/wallaby-output-inventory.json
```

For a durable POSIX mount, publication copies every inventoried file atomically and
revalidates its checksum:

```bash
wallaby_hires publish-local /path/to/session-output \
  /path/to/session-output/wallaby-output-inventory.json \
  /durable/wallaby/HIPASSJ1318-21
```

## Required production wiring

The current EAGLE graphs predate the execution-evidence contract and do not yet
place the verifier downstream of the real ASKAPsoft mosaic node. The no-download
test graph also creates zero-byte stand-ins, so inserting a production verifier
there would change the test graph's intended semantics.

Before Beampipe treats a production execution as successful, deployment must:

1. invoke `verify_output_products` after the mosaic command exits;
2. persist the resulting inventory as an execution artifact;
3. publish the inventoried files through `publish-local` to a durable mounted
   filesystem, or through an independently configured S3/Acacia publisher;
4. verify the destination hashes; and
5. report the destination URI and inventory digest to Beampipe Core.

This package intentionally does not infer object-store endpoints, credentials,
buckets, retention policy, or overwrite policy. Those are deployment secrets and
must be supplied by the production scheduler/profile. Until Core requires the
verified inventory and publication acknowledgement, scheduler completion alone is
not proof that durable science products exist.
