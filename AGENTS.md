# Codex instructions for mardorf-data-collector

## Projektübergreifende Betreiberregeln für Root-Freigaben — 19.10.2026

Diese ausdrücklich vom Betreiber festgelegten Regeln gelten für alle seine
Aufgaben, Projekte, Chats und zukünftigen Maßnahmen. Sie sind dauerhaft
versioniert zu erhalten und in neue Projektanweisungen zu übernehmen. Neue
Chats müssen die aktuellen Projektanweisungen und vor Netcup-Arbeiten die
[zentralen Serveranweisungen](https://github.com/janwohlers78/server-admin-control/blob/main/AGENTS.md)
laden. Die Ablage in Git aktiviert keine kontoweite Chat-Erinnerung: In einem
unabhängigen Chat muss diese Anweisung tatsächlich eingebunden oder gelesen
werden.

1. **Verständliche deutsche Begründung direkt auf der Freigabeseite.**
   Jede neue Anforderung von Root-Rechten für den Netcup-Server muss auf der
   Freigabeseite eine ausführliche Zusammenfassung und Begründung in klarer
   deutscher Sprache enthalten. Auch ohne technische Vorkenntnisse muss
   verständlich sein, welches konkrete Ziel verfolgt wird, welche Dienste,
   Dateien oder Einstellungen geändert werden, wozu Root benötigt wird und
   warum normale Benutzerrechte hierfür nicht ausreichen. Auswirkungen,
   gegebenenfalls eine kurze Betriebsunterbrechung und die vorgesehene
   Wiederherstellung sind verständlich zu erklären. Keine abgekürzte
   Kurzfassung, keine alleinige Befehlsliste oder Sammlung technischer Kürzel.
   Beim aktuellen OAuth-MCP-Verfahren gehört diese vollständige Erklärung in
   das Feld `intent`, das die Freigabeseite anzeigt. Eine Erklärung nur im Chat
   ersetzt den Text auf der Seite nicht. Bereits signierte Anträge dürfen
   nicht nachträglich umgeschrieben oder in ihrem Umfang erweitert werden.

2. **Zusätzliche ausdrückliche Freigabe für die genannten Eingriffe in
   andere Systeme.** Eine Root-Freigabe für Netcup ist keine pauschale Freigabe
   für Maßnahmen auf anderen Servern, Netzwerken oder Computern. Eine
   zusätzliche Freigabe des Betreibers ist erforderlich, wenn die konkrete
   Tätigkeit dort
   - disruptive Störungen verursachen könnte,
   - eine Passwort-, Code- oder Schlüsselsperre umgehen, unterwandern oder
     unbrauchbar machen würde oder
   - ein unbefugtes Eindringen in fremde Netzwerke beinhalten würde.

   Vor einem solchen Schritt den konkreten Eingriff, das betroffene System
   und die erwarteten Auswirkungen beschreiben und die zusätzliche Freigabe
   abwarten. Schweigen, verstrichene Zeit oder eine allgemeine Root-Freigabe
   ersetzen sie nicht. Diese Regel erteilt selbst keine Zugriffsberechtigung
   auf fremde Systeme.

Diese Regeln ergänzen die bestehende unabhängige Passkey-Freigabe; gewöhnliche
Befehle innerhalb eines bereits genehmigten Schritts benötigen dadurch keine
erneute Einzelbestätigung, solange keiner der genannten Zusatzfälle vorliegt.

## Aktuelle Betriebsgrenzen

GitHub Actions und sonstige GitHub-Automationen bleiben auf Betreiberanweisung
vom 19.10.2026 deaktiviert. Sämtliche B2-Lese- und Schreibzugriffe bleiben
stillgelegt. Historische Anleitungen erlauben keine Wiederaufnahme. Bei
Netcup-Arbeiten zusätzlich den aktuellen zentralen Admin-Handoff lesen und nur
den offiziellen Spotprognose-Server-OAuth-MCP-Verwaltungsweg benutzen.
