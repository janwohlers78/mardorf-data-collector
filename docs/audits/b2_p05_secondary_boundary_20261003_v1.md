# B2-P05: Collector-Steuerung und Stationsübergabe

Stand: 3. Oktober 2026. P05 ist weiter in Bearbeitung; die produktive Cloud-Umschaltung ist deaktiviert.

Der Collector kann seine Ausgangsdaten und Zyklusbelege bei ausdrücklich aktivierter Konfiguration aus B2 lesen. Bei einem Speicherfehler wird kein Wetterdatenbestand aus Git ausgewählt. Die Entscheidung, ob eine neue Erfassung erforderlich ist, verwendet kleine, geprüfte Metadaten in Git.

Die neue gemeinsame Stationsübergabe prüft unveränderliche Wunstorf- und ETNW-Lieferungen anhand der ursprünglichen Bytes und der Kennung des Collector-Laufs. Erst ein vollständig geprüftes Paar wird veröffentlicht. Neuere Einzellieferungen verändern dieses Paar nicht. Wiederholungen sind idempotent; ältere Paare setzen den aktuellen Zeiger nicht zurück.

Der private Abruf berücksichtigt die Belege und Nutzdaten des gewählten vollständigen Paars auch außerhalb des gleitenden Abrufzeitraums. Die private Verarbeitung kennzeichnet den Cloud-Transport ausdrücklich. Ein Git-Commit für außerhalb von Git gespeicherte Wetterdaten wird nicht behauptet. Bestehende Prüfungen der Stationsidentität und Aktualität bleiben aktiv.

Validierung: 391 öffentliche Regressionstests und 33 gezielte private Tests bestanden. Beide Prüfungen der Paket- und Workflow-Struktur bestanden. Der Test aus einem leeren Arbeitsordner umfasst Abruf, gemeinsame Stationsübernahme, Veröffentlichung der erzeugten Produkte und erneuten Abruf. Fehler bei Laufkennung, ursprünglichen Bytes und Aktualität verhindern die Übernahme. Dies ist ein lokaler Nachweis mit kontrollierten Testdaten, noch kein neuer produktiver B2-Nachweis.

Die bisherigen Quellen der beiden privaten Übergabemodule und des öffentlichen Strukturprüfers werden aufbewahrt. Die öffentlichen Quellkorrekturen besitzen einen gebundenen Nachfolgenachweis. Die ursprünglichen WP12/WP13/WP14- und Speichertransport-Nachweise werden nicht geändert.

Als Nächstes folgen die Umschaltpunkte in bestehenden Workflows und ein begrenzter Live-Test. Wissenschaftlicher Status: OPEN; operative Autorität: v16-c3-v9.
