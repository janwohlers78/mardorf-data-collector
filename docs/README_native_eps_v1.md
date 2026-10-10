# Direkte ICON-D2-EPS-Aufnahme v1

Expliziter Quellennachfolger der veralteten Open-Meteo-Metadatenbindung,
kein Austausch eines eingefrorenen Kalibrierungsmodells. Der öffentliche
Collector bleibt alleiniger Abrufbesitzer. Eingefrorene WP06–10-Anwendungen
und ihre API-Produkte bleiben unverändert.

Die bestehende EPS-Stufe nimmt DWD-Originale für die registrierten nativen
Dreistundenstützen 0–48 h auf, einschließlich aller 20 originalen Member 1–20.
U/V/Böen, Temperatur/Taupunkt/Feuchte, Druck, Niederschlag, Wolken, direkte
und diffuse Strahlung, CAPE, CIN und LPI bleiben mit sämtlichen Headern,
Einheiten und Akkumulations-/Maximalintervallen erhalten. Alle im jeweiligen
Original enthaltenen Felder werden archiviert, einschließlich vier Viertelstunden-
stützen je Member, wenn der Stundenblock diese enthält. Nur die abgeleiteten
Dreistunden-Zentralwerte wählen die exakte nominale Stütze; die zusätzlichen
Originalmeldungen bleiben unverändert erhalten. Die Auswahl ist eine explizite
Aufnahmematrix; sie behauptet weder alle angebotenen DWD-Parameter noch eine
stündliche native Vollaufnahme. Historische API-Felder werden nicht gelöscht.
Keine Zuordnung zu den alten API-IDs 0–19 und keine Interpolation.

Ein gemeinsamer, UUID- und SHA-gebundener Lookup bestimmt die native Zelle.
Das vollständige komprimierte Gitteroriginal und sein getrennt gehashter
Koordinatenpräfix bleiben erhalten. Vier aktive Aufgaben begrenzen Speicher
und Abrufe. Verzeichnisse werden je Parameter einmal gelesen. Nach Upload
prüfen kanonische Reads Originale, Projektionen und Memberwerte erneut.

Ein eigener `native-acquisition-seed-v1` bindet unveränderte Quellwerte und
individuelle kanonische Katalognachweise. Gesunde unveränderte Familien können
so auch bei einer anderen gestörten Quelle weitergeführt werden. Die kleine
Steuerreferenz `collector_models_acquisition_v1.json` gilt ausschließlich für
Abruffälligkeit, nicht als operative oder wissenschaftliche Freigabe. Die alte
`latest_success`-Autorität wird durch diesen Nachfolger nicht freigegeben.
Ein älterer oder unvollständig belegter Zustand ersetzt keinen jüngeren.

Die generische eingefrorene native Reader-Version behält ihren unveränderten
Raw-only-Ausgang für Dreiecksgitter. Die getrennte native EPS-Punktprojektion
schließt diese Lücke mit originalgebundenen Werten; sie ist kein numerischer
Fit. Fehlende oder widersprüchliche Quellen führen zu sichtbaren Fehlern.
Die EPS-Stufe ist auf zehn Minuten begrenzt; native Publikation weiterhin auf
zwölf, der gesamte Job auf 45. Erst reale Läufe qualifizieren diese Grenzen.
