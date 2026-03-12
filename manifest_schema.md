# Wallaby Hires manifest format

```json
{
  "inputs": { "credentials_ini_url": "https://example.com/casda.ini" },
  "sources": [
    {
      "source_identifier": "HIPASSJ1318-21",
      "ra_string": "13h18m00s",
      "dec_string": "-21.30.00",
      "vsys": 1234.5,
      "datasets": [
        {
          "name": "vis_12345_1",
          "staged_url": "https://data.example.com/vis_12345_1.ms.tar",
          "evaluation_file": "SB32736_eval.tar",
          "evaluation_file_url": "https://data.example.com/SB32736_eval.tar"
        }
      ]
    }
  ]
}
```