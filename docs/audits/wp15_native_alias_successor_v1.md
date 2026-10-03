# WP15 native alias successor

A real DWD vmax_10m response decodes as max_i10fg, paramId 237318, m s**-1, heightAboveGround 10, stepType max. V1 retained the raw response but returned raw_only. V2 binds this exact configured header to wind_gust and preserves the native name, unit, height, interval and original GRIB bytes. Contradictory metadata fails closed.

Frozen V1 stays byte-identical. One explicit successor module reuses the shared infrastructure and observation/EPS paths; later aliases extend configuration. No new workflow or forecast service; routine collection remains disabled. The private real canary supplies source hashes and capture/interpretation lineage.
