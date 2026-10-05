# Public collector architecture

The public repository owns source acquisition, integrity and verified delivery.
Forecast/calibration/decision/delivery logic belongs to the private companion.
The canonical project handoff exists only there; this file is public code navigation.

```mermaid
flowchart LR
  Trigger[Watchdog / fallback schedules / manual jobs] --> Runtime
  Runtime --> Provider[providers and WP13/WP15 collectors]
  Provider --> Raw[original bytes and source provenance]
  Raw --> Native[validated native / SI / Parquet]
  Native --> Storage[B2 immutable objects and manifests]
  Storage --> Transfer[verified delivery / receipts / control references]
  Transfer --> Private[private independent admission and causal readers]
```

| Package | Responsibility / interface | Dependencies and boundary |
|---|---|---|
| `runtime` | explicit lazy CLI, freshness/router, cloud environment | providers/contracts/transfer/storage; no forecast decision |
| `providers` | source HTTP/GRIB extraction and provider identity | source services and integrity/contracts |
| `integrity` | completeness/metadata/age/source-semantic audit | contracts; acquisition PASS is not operational usability |
| `contracts` | versioned identity, availability and semantic rules | standard library/config; no private policy |
| `wp13` | parameterized collection and bounded original processing | adapters, fields, core, provider APIs, original provenance |
| `wp15` | SVG/model/station ingestion, shared normalization, packages/readers | pinned processor/contracts/objects; readers do not acquire or normalize |
| `storage` | immutable object/hash/readback/CAS/workspace adapters | B2/S3 and GitHub; no domain promotion |
| `transfer` | monotonic private publication, receipts, atomic secondary batch | storage/contracts and GitHub; no forecasts |

Original/SI/Parquet/receipt/manifest forms serve distinct provenance/read purposes.
run/valid/available/first-received timestamps are not interchangeable.
Year packages are assembled publicly and privately admitted as bounded releases.

Configuration hierarchy: explicit deployment environment and CLI inputs select
operation/site/source; `config/dev03_cloud_runtime_v1.json` provides documented
B2-location defaults; Credentials have no defaults and are never written to Git.
Versioned config contracts determine field/source/temporal/integrity policy.
Private consumers verify exact source hashes: older WP13/WP15 versions and
compatibility bridges cannot be deleted based on static reachability alone.

Few rules: no private decision logic; no public weather payload artifacts;
no silent format/time changes; no new import cycles; preserve original provenance;
YAML orchestrates and Python implements processing. The protected lazy WP13
core/adapters cycle remains until an explicitly pinned successor is qualified.

Development uses Python 3.12 and `pip install -e '.[cloud,grib,stations,test]'`.
Runtime wheels continue to use the existing hash locks. Source aliases are
explicitly installed; `mardorf-collector --help` and existing script paths remain.
Config/data are checkout resources, not wheel contents. The 0.1.0 distribution
version is packaging metadata, not a scientific/project promotion.

Tests: `PYTHONPATH=src python -m unittest discover -s tests -v` and
`python tools/validate_prep09_layout.py`. Frozen PREP09 evidence is retained
unchanged; current bounded workflow changes have exact hash/replacement review in
`config/architecture_refactoring_v1.json`. No new historical validator chain.
