# WP15 – verlustfreie Kompression des vollständigen Feldinventars

Ein echter SVG-Halbtagestransfer mit 2.592 nativen Feldern benötigt im privaten
WP14-Speicher 1.857.812 zusätzliche Bytes. Der überwiegende Zusatzbedarf entsteht
durch repetitive, bisher unkomprimierte Detaildiagnostik. Der optionale Nachfolger
speichert deren vollständiges Inventar als hashgebundenes gzip-Rohobjekt.
Das ursprüngliche Inventar wird begrenzt und exakt wiederhergestellt; native
Wetterfelder, ursprüngliche HTTP-Antwort, Einheiten, Intervalle und Zeitstempel
bleiben unverändert. Das bereits ausgelieferte WP14 unterstützt diese gzip-
Objekte und optionalen Erweiterungen ohne Änderung seiner eingefrorenen Dateien.

Der echte Transfer sinkt damit auf 907.029 Bytes, etwa 51 Prozent weniger.
Vollständige Diagnoseinventare bleiben erhalten, einschließlich raw_only,
native_null und not_returned. Kleine GRIB-Inventare behalten ihren bisherigen
Aufbau. Die DWD-Originale sind bereits bzip2-komprimiert; erneutes gzip, bzip2,
lzma oder Zstd bringt dort keinen relevanten zusätzlichen Gewinn.

Die neue Release-V2 bindet den eingefrorenen V1-Vorgänger, unveränderte
CollectorV2-Projektion und einen gemeinsamen optionalen Statuscodec. Drei
Regressionstests prüfen identische Originale/native Daten, exakte Wiederherstellung,
Manipulations- und Dekompressionsgrenzen sowie unveränderte kleine/fehlgeschlagene
Lieferungen. Vollständige Collector-Suite: 355 Tests bestanden. Kein neuer
Workflow, keine Routineaktivierung und keine Änderung bestehender Daten.

Quelle des echten SVG-Originals: Erfassungslauf 36912167886 vom 2026-10-01,
Collector-Commit 43aca3c0de5b5073615f2cb90e4a8db75db54c8e; Original-Abruf
2026-10-01T19:09:22.578400Z. Die Interpretation bleibt getrennt davon.
