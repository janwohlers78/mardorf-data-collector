# Collector dependencies

`pyproject.toml` declares installable direct dependencies and optional features.
The six existing Python-3.12/Linux requirement locks remain immutable deployment
closures and include transitive wheels with SHA-256.

| Direct dependency | Category | Purpose / consumers |
|---|---|---|
| requests | Runtime | provider/GitHub HTTP and transfer/readback |
| urllib3 | Runtime, directly imported | Retry/HTTP configuration in provider and transfer paths |
| ecmwf-opendata | Runtime | ECMWF IFS acquisition |
| boto3, botocore | optional cloud | B2/S3 objects, configuration and explicit SDK errors |
| eccodes | optional GRIB | GRIB metadata/grid/field extraction |
| eccodeslib, eckitlib | platform GRIB closure | self-contained native Linux libraries |
| pyarrow | optional stations/models | native/SI/Parquet processing and reads |
| jsonschema | optional stations | field/format/domain validation |
| PyYAML | Test/CI | workflow/config syntax and layout validation |
| setuptools | Build/development | PEP-517/660 wheel/editable install |

Transitive closures: requests -> certifi/charset-normalizer/idna/urllib3;
ecmwf-opendata -> multiurl/tqdm/python-dateutil/pytz/six;
boto3 -> botocore/jmespath/s3transfer; jsonschema -> attrs/referencing/rpds-py/
jsonschema-specifications/typing-extensions; eccodes -> numpy/cffi/pycparser/findlibs
and native libraries. Absence of a direct import does not make these dead dependencies.

No documentation framework or additional test framework is required: Markdown,
Mermaid and standard-library unittest. The private project audit contains the
complete source/definition/import/workflow/config graph for this repository.

CI now resolves Runtime/CI/Cloud/GRIB/Parquet/station locks in one pip command and
caches the wheel closure using all requirement files. Exact versions/hashes remain.
Private consumers retain exact release/source pins; an editable install is a
development convenience, never a replacement for verified production pins.
