# WALLABY hi-res Beampipe operations

This documentation describes how to deploy and verify the `wallaby_hires` DALiuGE
components. Start with the operator runbook; it records both the procedure and the
boundary of the live end-to-end evidence.

- [Operator runbook](operator-runbook.md) — package deployment, local DALiuGE,
  Setonix prerequisites, staging, security, verification, and troubleshooting.
- [Execution manifest contract](manifest-contract.md) — production and
  no-download shapes, normalization, and validation.
- [Output integrity and publication](output-integrity.md) — the Wallaby inventory,
  the extended Beampipe Core report, and the trusted-publisher boundary.

The repository [README](https://github.com/jbwod/wallaby-hires-beampipe) contains
the science overview and graph catalogue. Only the current `*-beampipe.graph`
files should be considered deployment candidates; legacy, historical, and
component graphs are examples, not production configuration.
