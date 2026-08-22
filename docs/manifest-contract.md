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
          "evaluation_file": "SB32736_eval.tar",
          "evaluation_file_url": "https://data.example.test/SB32736_eval.tar",
          "evaluation_file_checksum_url": "https://data.example.test/SB32736_eval.tar.checksum",
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
          "evaluation_file": "SB32736_eval.tar",
          "evaluation_file_url": "https://data.example.test/SB32736_eval.tar",
          "evaluation_file_checksum_url": "https://data.example.test/SB32736_eval.tar.checksum"
        }
      ]
    }
  ]
}
```

Unknown prepared-metadata fields are ignored. If evaluation fields exist at both
levels, the SBID-level value wins. The graph pre-stage function returns a four-item
tuple: credentials path, generated CSV text, MS URL-list JSON, and evaluation
URL-list JSON. Evaluation downloads are deduplicated by URL.

## Structural validation

Run validation with the exact runtime interpreter:

```bash
/daliuge/.venv/bin/python -m wallaby_hires validate-manifest manifest.json
```

The validator enforces the following rules when `sources` is present:

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
checksum, or confirm that a URL will still be valid when the graph runs.

If `sources` is absent, the implementation enters legacy compatibility mode and
does not validate a nested dataset shape; even `{}` is accepted. This is not a
production admission check. New Beampipe manifests must always contain `sources`.

## Production download preflight

The common validator intentionally permits missing URLs for the no-download graph.
Before selecting a download/deploy graph, the submitting system must additionally
ensure that every dataset has a `staged_url`, and that each required SBID has an
evaluation filename and URL. It must also reject empty generated MS/evaluation URL
lists: the current download functions accept JSON `[]` as a no-op. Production
should supply the corresponding checksum URLs. The current download checks are
CASDA MD5 transfer-integrity checks; they are not signatures or proof of publisher
authenticity.

Use HTTPS and restrict outbound access to approved archive hosts at the deployment
boundary. HTTP is accepted for compatibility, and the package does not implement a
host allowlist.

## No-download semantics

`wallaby-hires_test-pipeline-nodownloads-beampipe.graph` is a control-plane smoke
test. It still runs manifest ingestion, normalization, CSV generation, scatter,
parset mixing, and Python imaging stubs, but both MS and evaluation download apps
are absent.

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
