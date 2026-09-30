# PREP09 / PR-A: Collector-Inventar

`repository_catalog_v1.json` erfasst den öffentlichen Quell-/Workflow-Unterbau
gegen den angegebenen Commit. Das gemeinsame Format entspricht dem privaten
Katalog: Dateien, Layer-Hinweise, Workflow-Einstiege, Pfad-/Import-Referenzen und
offene dynamische Abhängigkeiten.

Alle acht Collector-Workflows bleiben erhalten. Akquise, Watchdog, verifizierter
Transfer, Safe-Rollback-/Live-Proof-Pfade und CI werden weiterhin benötigt.
Kein öffentliches Modul wurde allein wegen Versionssuffix oder fehlendem
direkten Cron-Aufruf archiviert.

Der Watchdog dispatcht nur fällige, nicht laufende Collector-Jobs. Öffentliche
Schedules bleiben Fallbacks. Produktions- und geteilte L1/L2-Verträge wurden
durch diese Inventarisierung nicht verändert.

Der Katalog ist ein Snapshot, keine Workflow-Statusquelle, und enthält keine
Wetterpayloads oder Zugangsdaten. Erzeugung: gemeinsamer Scanner
`tools/repository_inventory.py` des privaten Projekts mit `--root <Collector-Checkout>`,
öffentlicher Repository-/Commit-Identität und öffentlichem Ausgabepfad.
