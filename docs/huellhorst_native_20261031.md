# Native Hüllhorst inputs: hourly collector callback

The additive `providers.huellhorst_native_v1.tick` callback checks the actual
latest nonfuture initialization of two already used native source families:
GEFS31members v0.2.0 and GFS deterministic v0.2.7, via their public Dynamical
Icechunk repositories. It retains every native data variable in the registered
regional stencil and all members/vertical dimensions, with the native lead axis
through120hours. No hourly interpolation, target-pressure conversion, fit,
generation reassignment or alteration of existing SVG/WP06/WP07/H1/R1 occurs.

Location and geometry are distinct: location `huellhorst-32609-v1`, new geometry
`huellhorst-rathaus-20261030-v1`,52.27194125N/8.67268261063196E,
129.800003m DHHN2016. This does not retrospectively relabel the old frozen R1
geometry. Ground elevation is not the unknown barometer height.

Call from the existing collector scheduler once an hour:

```python
from mardorf_collector.providers.huellhorst_native_v1 import tick
result = tick(own_output_directory, existing_lossless_native_writer,
              existing_OVH_backend, current=aware_utc_clock,
              runtime_binding=verified_native_runtime_manifest_reference)
```

The caller supplies the already qualified native runtime, the existing lossless
`save_native_subset` writer, and the OVH backend implementing put_bytes/get_bytes.
No credentials, B2 adapter, new daemon, cloud resource, workflow or dependency
installation is introduced. Ordinary collector imports do not import optional
Icechunk/xarray/numpy. The existing security5/native scientific environment can
provide those modules for isolated acquisition; its exact runtime manifest and
writer source must be pinned by the integration caller.
The receipt additionally binds the actual writer file/function source. A missing
runtime reference remains `runtime_bound=false`; acquisition success does not
silently qualify that runtime.

The callback preserves full original and receipt GET/readback before COMMITTED,
uses an exclusive own lock, records every attempt separately, reconciles an
existing identical original, and keeps changed snapshots as new revisions.
Current/asof, native initialization, retrieval, unknown publication/issue times,
units, native measurement attributes, dtype and shape remain separate. A stale
archive remains explicitly stale even if fetched now. A storage snapshot never
becomes proof of the operational model epoch or scientific qualification.
One unavailable source yields PARTIAL without hiding the other source's result.
No global current-forecast pointer or prospective cohort is changed.

Before any directory/data creation the existing8GiB reserve is checked on the
actual output filesystem. The complete materialized native field peak is
checked again before decoding/writing. Low space defers new work; no data fields
are dropped to fit. All original data/receipts are preserved.

Eleven isolated tests exercise future/stale clocks, native members and grid
identity, all-field/missing-value retention, source/snapshot revisions, immutable
byte verification, reserve rejection before directory creation, full publication
readback, hourly cadence and independent source failure. They pass using the
existing scientific binary runtime. The optional native suite is explicitly
skipped when that runtime is absent rather than silently installing packages.

Status31Oct2026: implementation and isolated tests are complete. Installation
in an active scheduler is **not yet performed**. A scheduled callback must use
the existing operating-role lock, crash recovery, byte budget, runtime binding
and authorized activation/drain process. A developed callback or a one-shot
capture must not be reported as a continuously running production collection.

Isolated real-source check31Oct2026: GEFS initialized00UTC yielded27native
fields with all31members; GFS initialized12UTC yielded25native fields. Both
used the existing bound security5 runtime and lossless writer. The originals
and receipts were packed and published through the existing OVH research
broker, then fully GET/readback-verified by size andSHA256. This was one
separate acquisition check, not scheduler activation or generation qualification.
Detailed object/runtime/job bindings stay in the private weather evidence.

Final integration checks additionally enforce the existing registered logical
archive prefix, independent runtime/writer/producer revisions, and per-attempt
freshness even when an immutable earlier receipt is reused. Captured clocks in
that earlier receipt are preserved. Raw data in `/dev/shm` is not treated as
durable: session cleanup removed unarchived temporary successors, so final
probe/staging uses a separate `/tmp` tmpfs and requires verified OVH publication.
The already verified first raw-input capsule remains retained.
