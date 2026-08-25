# Operator runbook

This runbook covers package deployment, DALiuGE connectivity, staging, Setonix
preflight, and end-to-end verification. Commands use package version `0.1.6`; use
the version pinned by the graph/deployment when that changes.

## Validation status

The following path was exercised against live local services:

- the `wallaby_hires` 0.1.5 package was installed and import-checked with
  `/daliuge/.venv/bin/python` in `dlg-tm`, `dlg-dim`, `dlg-nm1`, and `dlg-nm2`;
- translation used `http://dlg-tm.desk`;
- direct deployment reached `http://dlg-dim.desk:80` through Traefik, while the
  Translator Manager was told to use its Docker-network route `dlg-dim:8001`; and
- the current no-download Beampipe graph reached a terminal successful DALiuGE
  session with no error drops.

The production Setonix/SLURM graph was **not** run live for this revision because
the required Pawsey allocation and deployment-managed ASKAPsoft SIF were not
available. Its graph shape, environment guards, deployment-managed SIF reference,
and tests were validated statically. Treat the Setonix section below as a required
preflight, not as evidence of a completed science run.

The local run is manual evidence, not a CI guarantee. The repository's automated
suite validates Python, packaging, graph invariants, manifests, staging security,
and output helpers, but does not currently launch DALiuGE. Repeat the live smoke
test after changing graph topology, DALiuGE versions, port encodings, container
images, or deployment routing.

## Python and package names

Package metadata allows Python `>=3.10,<3.14`. CI tests Python 3.10 and 3.12; the
utility container is Python 3.10. The names differ by context:

| Context | Name |
|---|---|
| Python distribution / `pip` metadata | `wallaby-hires` |
| Python import | `wallaby_hires` |
| CLI | `wallaby_hires` |

Use the interpreter that launches DALiuGE PyFunc applications. A successful
install reported by another `python` or `pip` does not establish runtime
availability.

## Install in a DALiuGE virtual environment

From a trusted clone available inside the target environment:

```bash
make PYTHON=/daliuge/.venv/bin/python install
```

`make install` invokes that interpreter's pip with `--force-reinstall --no-deps`.
For the local four-container topology, run the command in **all** of `dlg-tm`,
`dlg-dim`, `dlg-nm1`, and `dlg-nm2`. Translation and scheduling can otherwise
succeed before a PyFunc lands on a node where the import is missing.

The validated local topology mounts the checkout at
`/dlg/workspace/wallaby-hires-beampipe` in all four containers. Its reproducible
development install is:

```bash
for container in dlg-tm dlg-dim dlg-nm1 dlg-nm2; do
  docker exec --workdir /dlg/workspace/wallaby-hires-beampipe "$container" \
    make PYTHON=/daliuge/.venv/bin/python install
done
```

Use the actual mounted checkout for another topology. Do not use an unqualified
`pip`, a separate `--prefix`, or only one manager container.

### Pinned wheel deployment

Build once in a supported, trusted build environment, check the distribution, and
install the same artifact everywhere:

```bash
python3 -m build --wheel
python3 -m twine check dist/*
sha256sum dist/wallaby_hires-0.1.18-py3-none-any.whl
```

Copy the wheel into each DALiuGE container, then run:

```bash
/daliuge/.venv/bin/python -m pip install \
  --disable-pip-version-check --force-reinstall \
  /tmp/beampipe_pallette-0.1.0-py3-none-any.whl \
  /tmp/wallaby_hires-0.1.18-py3-none-any.whl
/daliuge/.venv/bin/beampipe-publish --version
```

For an offline cluster, build and hash the wheel on a connected build host, move it
through the site's approved artifact path, and verify the hash before installation.
A PyPI install is appropriate only when the release exists and is pinned, for
example `wallaby-hires==0.1.18` and `beampipe-pallette==0.1.0`.

`docker exec` installs are development state and disappear when a container is
recreated. Production should bake the hash-pinned wheel into one pinned DALiuGE
base image and use that derived image for every manager and node service.

<figure class="bp-diagram" aria-labelledby="package-runtime-flow-title">
  <figcaption id="package-runtime-flow-title">
    <strong>Package-to-runtime flow.</strong> Build and verify one wheel, then make
    that identical artifact importable by the interpreter used by every executor.
  </figcaption>
  <ol class="bp-flow bp-flow--runtime">
    <li>
      <span class="bp-flow__eyebrow">Trusted build environment</span>
      <strong>Versioned source</strong>
      <span><code>wallaby-hires 0.1.6</code></span>
      <small>Supported Python and a reviewed commit</small>
    </li>
    <li>
      <span class="bp-flow__eyebrow">One immutable artifact</span>
      <strong>Wheel plus SHA-256</strong>
      <span><code>wallaby_hires-0.1.6-py3-none-any.whl</code></span>
      <small>Build once; verify before distribution</small>
    </li>
    <li class="bp-flow__fanout">
      <span class="bp-flow__eyebrow">Validated local topology</span>
      <strong>Install the same wheel everywhere</strong>
      <ul class="bp-executors" aria-label="DALiuGE services">
        <li><code>dlg-tm</code></li>
        <li><code>dlg-dim</code></li>
        <li><code>dlg-nm1</code></li>
        <li><code>dlg-nm2</code></li>
      </ul>
      <small>Use <code>/daliuge/.venv/bin/python</code></small>
    </li>
    <li>
      <span class="bp-flow__eyebrow">Graph execution</span>
      <strong>Control plane to executor</strong>
      <ol class="bp-runtime-path" aria-label="Runtime sequence">
        <li><code>TM</code> translates</li>
        <li><code>DIM</code> deploys and schedules</li>
        <li><code>NM</code> executes the PyFunc import</li>
      </ol>
      <small>Require the same package version and module path</small>
    </li>
  </ol>
  <p class="bp-diagram__note">
    <strong>Evidence boundary:</strong> live evidence covers the local no-download
    smoke only. Setonix/SLURM science execution has preflight/static validation
    only. Translation can succeed before a PyFunc reaches a node with a missing or
    stale package, and this flow does not prove ASKAPsoft or non-empty outputs.
  </p>
</figure>

## Verify every executor

Run both checks in each container or node environment:

```bash
/daliuge/.venv/bin/python -m wallaby_hires --version
/daliuge/.venv/bin/python -c \
  'import importlib.metadata as m, wallaby_hires; print(m.version("wallaby-hires"), wallaby_hires.__file__)'
```

The expected version and module location must be the same everywhere. Repeat this
check after every DALiuGE image rebuild or environment replacement.

## Local REST topology

Three addresses have different audiences in the validated Docker topology:

| Purpose | Validated address |
|---|---|
| Operator/test client to Translation Manager | `http://dlg-tm.desk` |
| Operator/Core direct deployment to DIM through Traefik | `http://dlg-dim.desk:80` |
| Translation Manager to DIM on the Docker network | `dlg-dim:8001` |

Do not give the Translator Manager a host-only `.desk` route if it cannot resolve
that name, and do not assume the externally routed port is the DIM's internal port.
Before a graph run, confirm TM inspection, DIM inspection/node discovery, graph
translation, session creation, terminal status, and an empty error-drop list.

## Shared staging workspace

`download_data_ms`, `download_data_eval`, `process_CSV_str`, and the ASKAPsoft
commands must resolve the same workspace. For Core-managed runs, Core owns this
value: it derives an absolute, non-root path for each execution below the DALiuGE
session workspace and exports that same value to the outer allocation and every
DALiuGE manager/node process. Do not configure one static service-wide staging
root. A standalone qualification may set its own disposable, execution-specific
path:

```bash
export WALLABY_HIRES_STAGING_ROOT=/shared/wallaby/beampipe/<execution-id>
```

The Setonix production entry points fail before data access when it is missing,
relative, resolves to `/`, or disagrees with the graph's staging DirectoryDROP.
Legacy/no-download helpers retain their explicit compatibility behaviour. The
nested production layout is:

```text
<staging-root>/<source_identifier>/<sbid>/beamNN/<dataset>.ms
<staging-root>/<source_identifier>/<sbid>/eval/LinmosBeamImages/<primary-beam>.cube.fits
```

Size storage for downloaded archives, extracted MeasurementSets (up to roughly
90 GB per source), intermediate images, final products, temporary extraction, and
inode headroom. Use a restrictive service account and umask, keep the root outside
the repository, and grant write access only to the DALiuGE runtime and publisher.

Retries are deliberately conservative:

- an MS is considered complete when its directory contains
  `.beampipe-extracted`, or legacy `table.dat` exists;
- an evaluation archive is considered complete only when the normalized
  `eval/LinmosBeamImages` directory contains exactly one non-empty `*.cube.fits`
  file; and
- an already downloaded archive is reused before network access only when a
  supplied checksum still matches.

Do not manufacture completion markers or bypass a checksum failure.

## Staging and secret safety

Production operators should apply all of the following controls:

- use HTTPS staged/evaluation/checksum URLs and an outbound host allowlist; the
  compatibility validator also accepts HTTP and does not implement a host allowlist;
- require checksum URLs for production staging, while recognising that CASDA MD5
  checks detect transfer corruption but are not publisher signatures;
- treat the complete execution manifest, DALiuGE graph configuration, session
  workspace, and `inputs/casda.ini` as secrets because signed URLs and credentials
  can be present even though error messages redact URL queries;
- use short-lived URLs, restrictive filesystem permissions, and a documented
  retention/cleanup policy after the execution and incident window;
- never place real credentials or signed URLs in tracked graphs, fixtures, shell
  history, tickets, or logs; and
- submit only safe source/SBID components and basenames. The runtime rejects path
  traversal, archive links, special archive members, non-HTTP(S) inputs, incomplete
  transfers, and checksum mismatches.

The package downloads `credentials_ini_url` atomically to `inputs/casda.ini` via a
mode-0600 temporary file. Protect its parent directory and clean it up according to
the retention policy. Signed staged URLs may not need that file; omit the URL when
it is unnecessary.

## Setonix preflight

The current graph assumes the Setonix `work` partition, `sbatch`, `squeue`,
`sacct`, `scontrol`, `scancel`, `srun`, `/scratch`, `/group/askap/modulefiles`, and the
`singularity/4.1.0-askap` module. It also requires the account and image variables
below in the environment of the **remote DALiuGE manager and app processes**.
Core-managed runs additionally provide the per-execution
`WALLABY_HIRES_STAGING_ROOT`; verify it from the launch receipt rather than
overriding it with static operator configuration.

```bash
export BEAMPIPE_SLURM_ACCOUNT=your-approved-allocation
export BEAMPIPE_ASKAPSOFT_SIF=/immutable/shared/path/askapsoft.sif
```

The account and SIF path are deployment configuration, not graph content. The SIF
must be immutable, readable on compute nodes, and managed through the site's
approved software path. Load the module in the DALiuGE launch environment because
the imcontsub, linmos, and mosaic commands call `singularity` directly:

```bash
module use /group/askap/modulefiles
module load singularity/4.1.0-askap
for command in sbatch squeue sacct scontrol scancel srun; do
  command -v "$command"
done
test -n "$BEAMPIPE_SLURM_ACCOUNT"
test -r "$BEAMPIPE_ASKAPSOFT_SIF"
test -w "$WALLABY_HIRES_STAGING_ROOT"
df -h "$WALLABY_HIRES_STAGING_ROOT"
df -i "$WALLABY_HIRES_STAGING_ROOT"
```

Install and import-check the pinned Wallaby wheel in the remote DALiuGE Python
environment, then repeat the import from an allocated compute node. Confirm the
allocation, partition, module version, bind paths, and a minimal site-approved
Singularity/SLURM smoke job before submitting science data.

### Nested imager job ownership

Scatter creates one nested imager job per flattened dataset, not one per source.
Each default child requests `work`, one node, two tasks, one CPU per task, 12 GB,
and 20 minutes. The checked-in one-source fixture contains six datasets, so it
would request six independently scheduled children: up to six concurrent nodes,
12 tasks, 72 GB aggregate memory, and 120 node-minutes of requested time.

The `run-setonix-imager` controller submits with `sbatch --parsable --hold` and
creates a mode-0700 `.beampipe-imager.<random>` directory below the beam
workspace. It accepts exactly one final parsable receipt; if a site banner makes
that receipt unusable, it recovers exactly one still-held job by the pre-recorded
unique session/beam name. Zero or multiple matches fail closed and remain held for
operator review. The controller writes the exact `child-job-id` (`job-id` or
`job-id;cluster`) before releasing the job. Other mode-0600 evidence includes the
unique name, copied parset, batch script, Slurm logs, release marker, and final
`state|exit-code`.

HUP, INT, TERM, scheduler-query failure, and accounting failure request `scancel`
for only that validated child ID. Success is reported only after `squeue` confirms
the ID absent; an unconfirmed cancellation is a fatal error with durable
`child-job-cancel-failure` evidence. Never clean up with `scancel -u`, a job-name
wildcard, or an unverified ID.

SIGKILL, compute-node loss, and power loss cannot run a process-local trap. An
attended qualification must therefore record every emitted
`BEAMPIPE_CHILD_JOB_ID` and inspect the exact lifecycle directories before outer
cleanup. Unattended production remains blocked until the deployment control plane
or a least-privilege reaper consumes those IDs and cancels any child whose outer
DALiuGE execution is no longer live.

Beampipe must fetch the graph from an immutable commit URL or verify its configured
SHA-256. EAGLE's `repoBranch` metadata is not an execution pin. Do not use legacy
or component graphs as a production substitute.

## End-to-end verification checklist

1. Verify identical `wallaby-hires` and `beampipe-pallette` versions and import
   paths in every executor.
2. Validate the manifest and apply the graph-specific production/no-download
   preflight in the [manifest contract](manifest-contract.md).
3. Confirm the graph bytes match the project-config SHA-256.
4. Inspect TM and DIM endpoints and confirm DIM reports the expected nodes.
5. Translate, create the session, deploy, and poll until a terminal state.
6. Require terminal success **and** no error drops; retain the session identifiers
   and sanitized poll evidence.
7. For production, confirm the terminal `beampipe-publish` node receives the
   post-mosaic completion sentinel, emits `beampipe-output-inventory.json`,
   re-reads the durable image and weights, and submits its execution-scoped Core
   receipt as described in [output integrity](output-integrity.md).
8. Confirm Core records the output-inventory artifact and terminal success. A
   scheduler/DALiuGE success alone is insufficient when output verification is
   required.

## Troubleshooting

| Symptom | Likely cause and action |
|---|---|
| `ModuleNotFoundError: wallaby_hires` | The package was installed with the wrong interpreter or is absent on that executor. Check version and `wallaby_hires.__file__` using `/daliuge/.venv/bin/python` in all four local containers. |
| TM translation succeeds, then a PyFunc fails on an NM | Package versions differ across services, or only manager containers were updated. Redeploy one pinned image/wheel everywhere. |
| Production manifest admission rejects a URL or archive field | Restage the inputs and supply the required HTTPS visibility/evaluation URLs and checksum URLs. Use `structural-no-download` only with the no-download graph. |
| Production rejects `WALLABY_HIRES_STAGING_ROOT` | Set one absolute, non-root, execution-specific shared path identically for DALiuGE and every compute node; do not rely on cwd. |
| HTTP 403 from a staged URL | The signed URL probably expired. Obtain a newly staged manifest unless a completed local MS/evaluation tree satisfies the documented retry marker. |
| Checksum mismatch or incomplete download | Do not bypass validation. Quarantine the affected archive/staging directory, obtain fresh staging evidence, and retry in a clean target. |
| Unsafe filename, archive member, path segment, or URL | The input violates the security boundary. Correct or reject the manifest/archive rather than repeatedly retrying it. |
| Evaluation archive produced zero or multiple PB cubes | The archive must contain exactly one non-empty `LinmosBeamImages/*.cube.fits` member for the selected SBID. Correct the evaluation artifact. |
| `BEAMPIPE_* are required`, `singularity: command not found`, unreadable SIF, or invalid Slurm account | Fix the environment inherited by remote DALiuGE apps, load the required module, and validate the allocation/SIF on a compute node. |
| Held imager receipt could not be resolved | Read `child-submission-unresolved`, query the exact recorded job name, and resolve every matching held job before continuing. Do not release an ambiguous match. |
| Outer execution stopped but a child may remain | Read only its validated `child-job-id` evidence, query that exact ID, and cancel that exact ID if still live. Do not select jobs by user or name. |
| Permission denied, `ENOSPC`, or unexplained extraction failure | Confirm the same writable shared staging mount on every node and check both bytes and inodes. |
| No-download run succeeds but products are empty | Expected: Python stubs create zero-byte placeholders. This graph cannot satisfy output verification. |
| `no outputs matched` or `output is empty` | The output root/naming is wrong, ASKAPsoft did not create science products, or a test graph was used. Do not publish or report success. |
| TM sees no DIM or deployment fails through Traefik | Check the audience-specific routes: external TM, external DIM deployment, and TM-internal `dlg-dim:8001` are distinct. |
