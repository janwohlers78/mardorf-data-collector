# WP09 original weather reports

The existing daily WP06/WP07/WP08 capture optionally includes
`--with-wp09-events`: complete Aviation Weather Center METAR JSON for ETNW
and EDDV, including every original field. It uses the existing schedule,
five-minute job bound and one monotonic CAS publication. No event classification,
fitting, negative lightning labels or additional forecast owner is introduced.

`config/wp09_events_v1.json` binds sources and station identities.
Originals and receipts are SHA256/byte checked after B2 publication;
`config/cloud_refs/wp09_events_ingress_v1.json` is transport control only.
Failures and empty responses retain their original bytes. Empty data are not
negative observations. Consumers must verify original report clocks and parse
observation, recent weather, vicinity and forecast trends separately.

One explicit research invocation of
`python -m mardorf_collector.runtime.wp09_events_v1 --history-output <outside-Git>`
retains complete annual IEM METAR CSV for 2024, 2025 and the fixed development
period through exclusive 2026-10-09. This is not a routine history downloader.
Its late retrieval clocks do not establish historical as-issued availability.
Neither archive nor daily METAR proves SVG 10-km lightning detector coverage.

Scientific NOT_READY; no safety or operational release.
