# PREP09 PR-B: Collector-Paket und Workflows

Neue Entwicklung nutzt `src/mardorf_collector`: `providers`, `contracts`,
`integrity`, `transfer` und `runtime`. Die 37 Paketoberflächen stehen
in `config/prep09_package_layout_v1.json` mit alten/neuen Pfaden und Quellhashes:
34 verschobene Implementierungen und drei Brücken zu bytegeschützten Vorläufern.
Validatoren bleiben an bisherigen Reproduktionspfaden. Das Skript
`record_provider_failure.py` bleibt unverändert, weil es Argumente bereits beim
Import verarbeitet; seine spätere Normalisierung benötigt einen eigenen Nachweis.

```sh
PYTHONPATH=src python -m mardorf_collector --help
PYTHONPATH=src python -m mardorf_collector audit-integrity --help
PYTHONPATH=src python -m mardorf_collector check-collection-due --help
```

CLI-Kommandos laden ausschließlich das ausgewählte Modul und erhalten dessen
bisherige Argumentverträge. Paket-/Unterpaket-Imports führen keine Jobs aus.
Der Watchdog-Befehl benötigt wie bisher ausschließlich die Standardbibliothek.
Alle Pakete laufen direkt aus dem Checkout mit `PYTHONPATH=src`; kein zusätzlicher
Installer, Service oder gemeinsames Runtime-Paket zwischen den Repositories.

Bei verschobenen Implementierungen sind alte Imports Aliase desselben Modulobjekts; alte CLI-Dateien leiten auf dessen
`main` weiter. Dadurch bleiben Klassenidentität, Globals und Test-Monkeypatches
konsistent. Die drei gepinnten Quellen `fetch_model_data.py`,
`fetch_dwd_additional_models.py` und `extend_model_horizon.py` bleiben an ihren
Originalpfaden bytegleich; deren Paketbrücken referenzieren dieselben Module.
WP03-I03/I04/I10-Validatoren prüfen weiterhin alle Sicherheitsbedingungen und
Originalhashes, lesen aber bei tatsächlich verschobenen Quellen die kanonischen
Implementierungen. Es gibt pro Funktion nur eine Implementierung. Adapterentfernung folgt
dem Layoutvertrag: nach WP11-Abnahme und vor der nächsten brechenden Paketversion,
mit eigenem PR, ohne aktive veränderbare Altpfad-Verbraucher und mit Paritäts-/CI-
Nachweis. Eingefrorene Reproduktion nutzt gepinnte Quellcommits.

`config/prep09_workflow_routes_v1.json` erfasst alle acht vorhandenen Workflows,
ihre Zuständigkeiten, Trigger, Rechte, Concurrency und Publikationsbereiche. Es ist
Navigation, kein neuer Scheduler. SVG-, Secondary-, Watchdog-, V3-Proof- und Smoke-
Workflows verwenden die Paket-CLI. Der exakt gepinnte `collect-models.yml` bleibt
bytegleich und nutzt Adapter. Die drei Fallback-Crons, Frische-Gates, privaten
Transferrechte, verifizierten Receipts und alle atomaren Batch-Gates bleiben erhalten.
Kein neuer Trigger oder Publikationskreislauf; Validator-Discovery bleibt erhalten.

Nachweise: `package_source_parity_v1.json` bestätigt die 34 mechanisch verschobenen
Quellen nach Normalisierung ausschließlich von Importen und Root-Auflösung sowie
die drei unveränderten geschützten Quellen.
`package_output_parity_v1.json` belegt einen bytegleichen vollständigen
Integritätsbericht für sechs Modellidentitäten bei festem UTC-Zeitpunkt. Die volle
Collector-Suite sowie Cross-Repository-Vertragsprüfungen bleiben erforderlich.

Neue Provider ergänzen `providers` mit Parser-/Identitätsprüfungen; Transferarbeit
bleibt in `transfer`. Bestehende Produktionsvertragsversionen werden nicht allein
wegen der Paketstruktur ersetzt. Neue unbekannte Dateien verschwinden nicht aus
`src/**`-CI oder Test-Discovery.
