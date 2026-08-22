# WALLABY hi-res manifest format

The graph consumes the nested Beampipe execution-manifest shape below. Datasets
belong to an SBID because an evaluation archive and its primary-beam products are
shared by all datasets in that scheduling block.

```json
{
  "inputs": {
    "credentials_ini_url": "https://example.com/casda.ini"
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
          "evaluation_file_url": "https://data.example.com/SB32736_eval.tar",
          "evaluation_file_checksum_url": "https://data.example.com/SB32736_eval.tar.checksum",
          "datasets": [
            {
              "name": "vis_12345_1.ms.tar",
              "staged_url": "https://data.example.com/vis_12345_1.ms.tar",
              "checksum_url": "https://data.example.com/vis_12345_1.ms.tar.checksum"
            }
          ]
        }
      ]
    }
  ]
}
```

Validation rules:

- `sources`, `sbids`, and `datasets` must be non-empty arrays when `sources` is
  present.
- `source_identifier` and `sbid` must each be one portable filesystem component;
  absolute paths, separators, traversal components, and control characters are
  rejected.
- `ra_string`, `dec_string`, and finite numeric `vsys` are required for every
  source. `vsys` may be a JSON number or Core's numeric-string representation.
- Every dataset requires a safe basename in `name`, `dataset_id`, or
  `visibility_filename`. `staged_url` is required by download graphs but may be
  absent from no-download test manifests.
- Evaluation filename/URL/checksum fields may be present on their shared SBID
  object or repeated on Core's dataset records; both forms normalize identically.
- Evaluation and checksum URLs, when supplied, must use HTTP(S).
- The legacy `inputs.input_csv_url` plus `staged` URL-list shape is accepted only
  when `sources` is absent. It is retained for old graphs and should not be used by
  new Beampipe projects.

Use `wallaby_hires validate-manifest MANIFEST.json` to validate a file before graph
submission.
