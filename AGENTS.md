# Codex instructions for mardorf-data-collector

## Projektübergreifende Betreiberregeln für Root-Freigaben — 20.10.2026

Diese ausdrücklich vom Betreiber festgelegten Regeln gelten für alle seine
Aufgaben, Projekte, Chats und zukünftigen Maßnahmen. Sie sind dauerhaft
versioniert zu erhalten und in neue Projektanweisungen zu übernehmen. Neue
Chats müssen die aktuellen Projektanweisungen und vor Netcup-Arbeiten die
[zentralen Serveranweisungen](https://github.com/janwohlers78/server-admin-control/blob/main/AGENTS.md)
laden. Die Ablage in Git aktiviert keine kontoweite Chat-Erinnerung: In einem
unabhängigen Chat muss diese Anweisung tatsächlich eingebunden oder gelesen
werden.

1. **Jede Aktion mit Root-Rechten als eigener Stichpunkt auf der Freigabeseite.**
   Jede neue Root-Freigabe muss eine übersichtliche Liste der geplanten Aktionen
   enthalten. Jede Aktion, für die Root-Rechte benötigt werden, erhält einen
   eigenen kurzen, prägnanten Stichpunkt. Konkret benennen, was an welchem
   Dienst, welcher Datei oder welcher Einstellung ausgeführt werden soll.
   Unterschiedliche Root-Aktionen nicht in einem Sammelpunkt zusammenfassen.
   Natürliches, korrektes und auch für Laien verständliches Deutsch verwenden;
   keine reine Befehlsliste oder Sammlung technischer Kürzel.
   Nur wenn es für das Verständnis nötig ist, einen kurzen Kontextsatz oder
   eine knappe Erläuterung ergänzen. Erforderliche Neustarts und Maßnahmen zur
   Wiederherstellung ebenfalls als eigene Aktionen aufführen.
   Beim OAuth-MCP-Verfahren steht diese Aktionsliste im Feld `intent`, das
   direkt auf der Freigabeseite angezeigt wird. Eine Erklärung nur im Chat
   ersetzt sie nicht. Diese Vorgabe vom 20.10.2026 ersetzt die bisherige Pflicht
   zur ausführlichen Fließtextbegründung. Bereits signierte Anträge dürfen
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

3. **Root-Freigabelink immer im Chat und zusätzlich per E-Mail.**
   Am 19.10.2026 erneut ausdrücklich vom Betreiber beauftragt: Jeden neuen,
   noch handlungsfähigen Root-Freigabelink sofort sowohl im Chat als auch über
   die verbundene Outlook-E-Mail-Anbindung an die im privaten zentralen
   Admin-Runbook hinterlegte Betreiberadresse senden. Diese Anweisung ist die
   ausdrückliche, fortgeltende Beauftragung für diese Freigabe-E-Mails; dafür
   nicht nochmals um eine Versandfreigabe bitten. Betreff mit `[Codex]`,
   vollständiger identischer Link und verständliche deutsche Beschreibung des
   konkreten Schritts. Annahme durch die Mail-API und passende Kopie unter
   „Gesendet“ prüfen; sie beweisen keinen Empfang oder Geräte-Push.
   Versandfehler offen melden, den Chat-Link trotzdem bereitstellen und vor
   unklaren Wiederholungen die gesendeten Nachrichten prüfen. Keine neue
   Root-Freigabe nur für einen erneuten Mailversand erzeugen. Bereits bestätigte
   oder abgelaufene Anträge nicht als noch zu genehmigen darstellen. Keine
   Serverprotokolle, Zugangsdaten oder Schlüssel in diese E-Mails aufnehmen.

Diese Regeln ergänzen die bestehende unabhängige Passkey-Freigabe; gewöhnliche
Befehle innerhalb eines bereits genehmigten Schritts benötigen dadurch keine
erneute Einzelbestätigung, solange keiner der genannten Zusatzfälle vorliegt.

## Aktuelle Betriebsgrenzen

GitHub Actions und sonstige GitHub-Automationen bleiben auf Betreiberanweisung
vom 19.10.2026 deaktiviert. Sämtliche B2-Lese- und Schreibzugriffe bleiben
stillgelegt. Historische Anleitungen erlauben keine Wiederaufnahme. Bei
Netcup-Arbeiten zusätzlich den aktuellen zentralen Admin-Handoff lesen und nur
den offiziellen Spotprognose-Server-OAuth-MCP-Verwaltungsweg benutzen.
