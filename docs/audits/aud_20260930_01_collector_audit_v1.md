# AUD-20260930-01: Collector-Unterbau vor WP05

Basis `7e9d663fbf3c75c78487c82cf80719ebfb9c8a63`. Status FIX_IMPLEMENTED;
finale Validate-/Merge-Abnahme wird einmalig im privaten Projekt-Handoff gespeichert.

- F02/P2: Nicht exakt darstellbare Submikrosekunden wurden vom UTC-/Lead-Parser
  abgeschnitten. Präzisionsguard erhält gültige Instanten/Leads; lossy Zeiten
  werden vor Availability-/Pointer-/Parent-Identitätsbildung abgelehnt.
- F04/P1: Bestehende ungültige, duplicate-key oder zeitlose Transfer-Pointer wurden
  als fehlend/älter behandelt. Nur404/None bedeutet fehlend; vorhandene Pointer
  müssen exakt lesbare, nicht leere unique-key JSON-Objekte mit Zeitbezug sein.
  Fehler stoppen Publikation. V3-Loader übernimmt denselben strikten Reader.
  Parent-bound Monotonie und tatsächlicher Ersttransfer funktionieren weiterhin.
- F05/P1: Availability-Validator akzeptierte Runtime-received für unbekannte
  Registry-Capability. Constructor-/Validator-Präzedenz vereinheitlicht; Runtime
  erfordert native_available + required. Alle8unbekannten Subjects adversarial
  geprüft; gültige received-Zero/Causal-Time bleiben erhalten.
- F06/P2: Paket-/Quellkorrektur- und Frozen-Bridge-Verifikation wird zusätzlich in
  vorhandener unittest-Discovery ausgeführt. Keine neue Workflow-/Cron-Datei.

Lokale vollständige Suite309Tests bestanden, darunter9Audit-Prüfungen.
Historische Migration-Parität bleibt erhalten. Explizite Korrekturbindungen in
`docs/inventory/package_source_corrections_v1.json` binden Findings, alte ASTs und
aktuelle Source-/AST-Hashes. Protected Provider-Dateien und collect-models.yml
bleiben byteidentisch. Kontrollerhalt prüft alle8Workflows und drei Frozen-Brücken.
Keine neue Akquise, kein Retuning, kein Transfer- oder Enablement-Dispatch.

Reproduktion: `PYTHONPATH=src python -m unittest discover -s tests -v` und
`PYTHONPATH=src python tools/validate_prep09_layout.py`. Rücknahme am Audit-Basis-
Commit, zusammen mit Correction-Manifestbindung; keine getrackten Daten geändert.
Dies ist fokussierte Quell-/Correctness-Prüfung, kein Exhaustiv- oder Security-Audit.

Der erste Validate-Lauf36763151220 stoppte wegen fehlendem yaml-Modul: lokale Umgebung enthielt private CI-Abhängigkeiten. Public CI installiert jetzt PyYAML6.0.3 hashgepinnt aus requirements-ci.txt. Der Lockfile-Pfad gehört zur PR-CI-Coverage; deklarierte Korrekturbindung erlaubt ausschließlich diese neue CI-Path-Zeile und prüft alle übrigen alten Controls exakt. Produktive requirements-runtime.txt und sämtliche Akquise-Trigger bleiben unverändert. Finale Abnahme in einer separaten ausschließlich gepinnten CI-Umgebung.
