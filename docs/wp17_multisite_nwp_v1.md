# WP17: gemeinsamer ortsgebundener Wetterabruf

SVG bleibt primärer Wetter-/Kite-Ort. Volmerdingsen ist ausschließlich ein
lokaler Wetterort. Der bestehende WP06/WP07-Abruf erhält eine zusätzliche,
verlustfrei archivierte Antwort mit zwei ausdrücklich angefragten Koordinaten.
Ein Abruf, ein vorhandener Job und eine vorhandene Veröffentlichungs-Transaktion
bedienen beide Orte. Eingefrorene WP06/WP07-Antworten und Protokolle bleiben
unverändert. Die neue private Anwendung trainiert nicht.

`wp17_multisite_nwp_v1.json` bindet Orte, Produkt, Einheiten, h0–120 und alle14
angefragten Wetterfelder. Weitere zurückgegebene Felder bleiben erhalten.
Initialisierung und tatsächlicher Abruf sind getrennte Uhren; ein alter oder
fehlgeschlagener Lauf wird nicht als frischer Lauf weitergeschaltet. API-Wetter
über h72 ist im Nachfolger zunächst Rohinformation, keine fachliche Freigabe.

Der vorhandene manuelle historische Workflow unterstützt
`multisite-nwp-history` mit registriertem abgeschlossenem Kalenderbereich.
Anfragen werden global gedrosselt, alle Fehlerantworten gespeichert. Nach
Ratenbegrenzung kann `history(..., previous=...)` in einem neuen, vor dem Abruf
registrierten Versuch geprüfte Originale wiederverwenden. Vorgänger und
Fehlversuche bleiben unveränderlich erhalten. Ein Original-ZIP bündelt den
Bestand, statt pro Ort, Feld und Stunde Speichertransaktionen anzulegen.

Abnahme11.10.2026: zwei eigene Gitterpunkte nachgewiesen, 933/937 historische
Initialisierungen2024-03-14 bis2026-10-06. Vier00UTC-Läufe2025-08-05/06/08/09
fehlen laut Anbieter. 653 Repository-Tests bestanden. Mehrortkalibrierung und
SVG-Wetterchancen sind experimentell; wissenschaftlich NOT_READY, keine
Sicherheitsfreigabe, kein privater Anbieterabruf und kein neuer Routine-Cron.
