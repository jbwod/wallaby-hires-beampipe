# Execution manifest contract

The Beampipe graphs consume a nested JSON execution manifest. Datasets are grouped
under an SBID because one evaluation archive and its primary-beam products can be
shared by every dataset in that scheduling block.

## Canonical shape

```json
{
  "inputs": {
    "credentials_ini_url": "https://secrets.example.test/casda.ini"
  },
  "sources": [
    {
      "source_identifier": "HIPASSJ1318-21",
      "ra_string": "13h18m00s",
      "dec_string": "-21.30.00",
      "vsys": 1234.5,
      "sbids": [
        {
          "sbid": "32736",
          "evaluation_file": "calibration-metadata-processing-logs-SB32736_2025-04-21-063210.tar",
          "evaluation_file_url": "https://data.example.test/calibration-metadata-processing-logs-SB32736_2025-04-21-063210.tar",
          "evaluation_file_checksum_url": "https://data.example.test/calibration-metadata-processing-logs-SB32736_2025-04-21-063210.tar.checksum",
          "datasets": [
            {
              "name": "HIPASSJ1318-21_SB32736_F00_B01.ms.tar",
              "staged_url": "https://data.example.test/HIPASSJ1318-21_SB32736_F00_B01.ms.tar",
              "checksum_url": "https://data.example.test/HIPASSJ1318-21_SB32736_F00_B01.ms.tar.checksum"
            }
          ]
        }
      ]
    }
  ]
}
```

`inputs` may be empty when the staged URLs do not require a CASDA credentials
file. `credentials_ini_url`, when present, is downloaded to the DALiuGE session's
`inputs/casda.ini`; it is not a general-purpose secret-reference mechanism.

## Beampipe Core dataset records

Core normally preserves prepared dataset metadata in each `datasets` entry. The
package accepts `dataset_id` or `visibility_filename` instead of `name`, a numeric
string for `vsys`, and evaluation fields repeated on each dataset. This is one
`sources[]` object from that manifest:

```json
{
  "source_identifier": "HIPASSJ1318-21",
  "ra_string": "13h18m00s",
  "dec_string": "-21.30.00",
  "vsys": "1234.5",
  "sbids": [
    {
      "sbid": "32736",
      "datasets": [
        {
          "dataset_id": "HIPASSJ1318-21_SB32736_F00_B01.ms.tar",
          "visibility_filename": "HIPASSJ1318-21_SB32736_F00_B01.ms.tar",
          "staged_url": "https://data.example.test/beam.ms.tar",
          "checksum_url": "https://data.example.test/beam.ms.tar.checksum",
          "evaluation_file": "calibration-metadata-processing-logs-SB32736_2025-04-21-063210.tar",
          "evaluation_file_url": "https://data.example.test/calibration-metadata-processing-logs-SB32736_2025-04-21-063210.tar",
          "evaluation_file_checksum_url": "https://data.example.test/calibration-metadata-processing-logs-SB32736_2025-04-21-063210.tar.checksum"
        }
      ]
    }
  ]
}
```

Unknown prepared-metadata fields are ignored. If evaluation fields exist at both
levels, the SBID-level value wins. The graph pre-stage function returns a four-item
tuple: credentials path, generated CSV text, MS URL-list JSON, and evaluation
URL-list JSON. Evaluation downloads are deduplicated per source, SBID, and URL.

## Admission modes

Run fail-closed Setonix production validation with the exact runtime interpreter:

```bash
/daliuge/.venv/bin/python -m wallaby_hires validate-manifest manifest.json
```

`setonix-production` is the CLI, `validate_manifest`, and
`prestage_manifest_inputs` default. It enforces all structural rules plus:

- exactly one source and at least one dataset;
- one evaluation archive description per SBID, either on the SBID or repeated
  identically on every dataset in that SBID; its case-sensitive basename must be
  `calibration-metadata-processing-logs-SB<exact-sbid>_YYYY-MM-DD-HHMMSS.tar`;
- a required HTTPS `staged_url` and HTTPS `checksum_url` for every visibility
  dataset;
- required HTTPS evaluation archive and checksum URLs for every SBID; and
- one generated visibility download per dataset and one generated evaluation
  download per SBID.

Missing fields, legacy shapes, HTTP URLs, conflicting repeated evaluation
metadata, duplicate SBIDs, and empty generated download lists fail admission
before credentials are fetched or downloads are started.

The structural rules shared by both modes are:

- `sources`, `sbids`, and `datasets` are non-empty arrays.
- Every source has a portable single-component `source_identifier`, non-empty
  `ra_string` and `dec_string`, and a finite numeric `vsys`. `vsys` may be a JSON
  number or numeric string.
- Every SBID is a portable single path component.
- Every dataset has a safe basename in `name`, `dataset_id`, or
  `visibility_filename`. Absolute paths, traversal, separators, drive-qualified
  paths, and control characters are rejected.
- Staged, evaluation, and checksum URLs, when supplied, are HTTP(S). Credential
  URLs are checked only when pre-staging opens them.
- Evaluation filenames are safe basenames. Evaluation filename, URL, and checksum
  may be on the SBID or repeated in Core dataset records.

Validation is structural and does not fetch URLs, validate credentials, prove a
checksum, inspect the evaluation archive, or confirm that a URL will still be
valid when the graph runs.

After the evaluation archive is downloaded, runtime extraction accepts only a
non-empty regular member below `LinmosBeamImages/` whose name ends in
`.cube.fits`. Zero or multiple matches fail before extraction is published, and
primary-beam resolution never falls back to the first lexicographic candidate.
This runtime check is necessarily separate from manifest admission because the
current Core manifest describes the archive but does not name its inner member.

For the no-download control-plane graph, select the weaker policy explicitly:

```bash
/daliuge/.venv/bin/python -m wallaby_hires validate-manifest \
  manifest.json --mode structural-no-download
```

The no-download graph calls `prestage_manifest_inputs_no_download`, which binds
that policy explicitly. If `sources` is absent, only this mode enters legacy
compatibility and accepts the old flat `inputs`/`staged` shape. New Beampipe
manifests should always contain `sources`.

Production download checks use CASDA MD5 evidence for transfer integrity; they
are not signatures or proof of publisher authenticity.

Use HTTPS and restrict outbound access to approved archive hosts at the deployment
boundary. HTTP is accepted only by structural/no-download compatibility mode, and
the package does not implement a host allowlist.

## No-download semantics

`wallaby-hires_test-pipeline-nodownloads-beampipe.graph` is a control-plane smoke
test. It explicitly selects `structural-no-download` admission and still runs
manifest ingestion, normalization, CSV generation, scatter, parset mixing, and
Python imaging stubs, but both MS and evaluation download apps are absent.

Consequently:

- `staged_url` may be absent and the generated MS URL list may be empty;
- no staged dataset, checksum, archive, MeasurementSet, or PB FITS is validated;
- `credentials_ini_url` should be omitted because pre-staging will still download
  it even though the data-download apps are absent;
- the stubs do not read real MeasurementSets and create zero-byte placeholder
  files; and
- the corresponding Beampipe project must keep output verification
  `required: false` for this graph.

A successful no-download session proves that the selected manifest/control-plane
path can translate, deploy, and execute. It does **not** prove data staging,
ASKAPsoft, Setonix, non-empty science products, durable publication, or the Core
output-verification transition.

The legacy `inputs.input_csv_url` plus `staged` URL-list shape exists for old
graphs and should not be used by new Beampipe projects.
