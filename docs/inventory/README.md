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

Aktuelle Paketpfade und Workflow-Verantwortung: `package_layout_v1.md`,
`config/prep09_package_layout_v1.json` und `config/prep09_workflow_routes_v1.json`.
Die Paritätsbelege stehen in `package_source_parity_v1.json` und
`package_output_parity_v1.json`. Nach PR-B bilden die Altdateien ausschließlich
Adapter; im Katalog werden deren literale Paketimporte statisch aufgelöst.
