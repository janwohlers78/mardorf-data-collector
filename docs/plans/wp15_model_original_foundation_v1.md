# WP15-I02: Modelloriginale in begrenzten Objekten

Diese Grundlage ist noch nicht in Routinejobs aktiviert. WP15-I02 bleibt offen.

`storage.parents.ParentStore` speichert eine Originaldatei unverändert in
8-MiB-Blöcken. Ein kleiner unveränderlicher Manifest bindet Reihenfolge,
Gesamtgröße und SHA256 der vollständigen Datei. Normale Metadatenleser müssen
die große Rasterdatei nicht rekonstruieren. Eine ausdrücklich angeforderte
Rekonstruktion wird erst nach vollständiger Integritätsprüfung sichtbar.

`wp13.model_originals_v1.Capture` kann vorhandene HTTP-Antworten aufnehmen und
bereits erzeugte native Punktmetadaten über ihre Quellenidentität zuordnen.
Requests liest den Antwortkörper einmal; Wetterabruf und Capture verwenden
denselben unveränderten Körper. Es werden keine zusätzlichen Provideranfragen
ausgeführt. Requests-Fehler bleiben Fehler des ursprünglichen Abrufs;
Speicherfehler werden separat dokumentiert und verwerfen keine gültige Antwort.
Authentifizierungsparameter werden nicht als Quellenmetadaten gespeichert.
HTTP-Range-Metadaten sind Bestandteil der unveränderlichen Herkunftsreferenz.

Die EPS-Metadaten hashen kanonisches JSON statt ursprünglicher HTTP-Bytes.
Beide Identitäten bleiben ausdrücklich getrennt. Auch vollständige vorhandene
Ensemble-Metadaten bleiben erhalten. Eine solche Zuordnung ist noch keine
native Readerabnahme: der Bericht trägt `native_reader_admission=NOT_QUALIFIED`.

Noch auszuführen: Integration in vorhandene Provider-/SDK-Einstiegspunkte,
Speicherung im Routine-B2-Ablauf, formeller Empfänger für große Originalreferenzen,
Prüfung tatsächlicher Standort-/Raster-/Member-Metadaten und Abnahme anhand
automatischer Routinejobs. Die eingefrorenen Collector-, Feld- und
Dienstverträge werden dadurch nicht stillschweigend verändert.

Elf gezielte Tests prüfen tatsächliche Originalbytes über Blockgrenzen,
unvollständige Rekonstruktion, unveränderte Requests-Nutzung, Quellendigest-
Zuordnung, EPS-Identitäten, unveränderliche Herkunftsmetadaten und Fehlergrenzen. Layoutprüfung: 9 Workflowkontrollen
und 3 geschützte Providerbrücken unverändert. Wissenschaftliches Gate OPEN;
v16-c3-v9 und vorhandener Dienst bleiben maßgeblich.
