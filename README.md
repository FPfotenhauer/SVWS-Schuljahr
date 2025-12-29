# SVWS-Schuljahr

Tool das SVWS-Schulungsdatenbanken um ein Schuljahr versetzt.

## Ziele

1. Alle Schuljahresabschnitte in der Tabelle "Schuljahresabschnitte" beginnend mit dem höchsten Abschnitt um ein Jahr erhöhen.
2. Alle Datumswerte bei Schülern und Lehrkräften um ein Jahr erhöhen.

## Installation

### Voraussetzungen

- Python 3.12+
- MariaDB 10.11+ oder höher
- pip (Python Package Manager)

### System-Abhängigkeiten

Für die MariaDB-Unterstützung werden folgende Systembibliotheken benötigt:

```bash
sudo apt-get install libmariadb-dev libmariadb3 python3.12-dev
```

### Python-Abhängigkeiten

Installieren Sie die erforderlichen Python-Pakete:

```bash
pip install mariadb
```

Oder verwenden Sie die virtuelle Umgebung des Projekts:

```bash
.venv/bin/pip install mariadb
```

## Konfiguration

### config.json

Die Datei `config.json` enthält die Datenbankverbindungskonfiguration:

```json
{
  "database": {
    "host": "localhost",
    "port": 3306,
    "user": "dbuser",
    "password": "",
    "database": "svws",
    "autocommit": true
  },
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  }
}
```

**Konfigurationsparameter:**
- `host`: Datenbankserver-Hostname (Standard: localhost)
- `port`: Datenbankserver-Port (Standard: 3306)
- `user`: Datenbankbenutzer
- `password`: Datenbankpasswort
- `database`: Datenbankname
- `autocommit`: Automatisches Commit nach Queries (Standard: true)

## Verwendung

### Grundlegende Verwendung

```python
from svws_schuljahr import create_connection_from_config, increment_schuljahresabschnitte_jahr

# Verbindung erstellen
db = create_connection_from_config("config.json")

if db and db.connect():
    # Schuljahresabschnitte um ein Jahr erhöhen
    if increment_schuljahresabschnitte_jahr(db):
        print("✓ Schuljahresabschnitte erfolgreich aktualisiert")
    else:
        print("✗ Fehler beim Aktualisieren")
    
    db.disconnect()
```

### Script ausführen

Das Skript kann direkt ausgeführt werden und führt automatisch Tests durch:

```bash
python3 svws_schuljahr.py
```

Oder mit der virtuellen Umgebung:

```bash
.venv/bin/python svws_schuljahr.py
```

## Funktionalität

### MariaDBConnection Klasse

Verwaltet die Verbindung zur MariaDB-Datenbank:

**Hauptmethoden:**
- `connect()`: Verbindung zur Datenbank herstellen
- `disconnect()`: Verbindung trennen
- `execute_query(query, params)`: SELECT-Query ausführen
- `execute_update(query, params)`: INSERT/UPDATE/DELETE ausführen
- `commit()`: Transaktionen bestätigen
- `rollback()`: Transaktionen rückgängig machen
- `get_cursor()`: Datenbankzeiger abrufen

**Context Manager Unterstützung:**
```python
with MariaDBConnection(host="localhost", user="root", password="password", database="SVWS") as db:
    results = db.execute_query("SELECT * FROM Schuljahresabschnitte")
```

### Konfigurationsladen

`load_config(config_path)`: Lädt die Konfiguration aus einer JSON-Datei

**Parameter:**
- `config_path`: Pfad zur config.json (Standard: "config.json")

**Rückgabewert:**
- Dictionary mit Konfigurationsdaten

### Schuljahresabschnitte-Inkrementierung

`increment_schuljahresabschnitte_jahr(db, do_commit=True)`: Erhöht die Jahr-Spalte in der Schuljahresabschnitte-Tabelle um 1

**Funktionsweise:**
1. Ruft alle unterschiedlichen Jahre aus der Tabelle ab, sortiert in absteigender Reihenfolge
2. Verarbeitet jedes Jahr vom höchsten zum niedrigsten
3. Prüft auf Constraint-Verletzungen bevor das Jahr aktualisiert wird
4. Aktualisiert alle Datensätze für das entsprechende Jahr
5. Committed die Transaktion nach erfolgreicher Aktualisierung (abschaltbar via `do_commit=False`)

**Rückgabewert:**
- `True` bei Erfolg
- `False` bei Fehler (mit automatischem Rollback)

### Schueler-Datumsfelder-Inkrementierung

`increment_schueler_dates(db, do_commit=True)`: Erhöht alle Datumsfelder in der Tabelle Schueler um 1 Jahr.

**Betroffene Spalten:**
- Geburtsdatum, Religionsabmeldung, Religionsanmeldung, Schulwechseldatum
- BeginnBildungsgang, AnmeldeDatum, EndeEingliederung, EndeAnschlussfoerderung
- SprachfoerderungVon, SprachfoerderungBis

**Eigenschaften:**
- Nutzt `DATE_ADD` für korrekte Jahresverschiebung
- NULL-sicher (NULL bleibt NULL)
- Ein Bulk-Update für alle Felder

### Schueler-Abschlussdatum (VARCHAR) Inkrementierung

`increment_schueler_abschlussdatum(db, do_commit=True)`: Erhöht das Feld Abschlussdatum (VARCHAR, Format D.M.YYYY/ DD.MM.YYYY) um 1 Jahr.

**Eigenschaften:**
- Parsen mit `STR_TO_DATE`, Jahresinkrement via `DATE_ADD`
- Ausgabe im ursprünglichen deutschen Datumsformat
- Nur Werte, die das erwartete Datumsregex erfüllen, werden angepasst

### Schueler-Jahresfelder (Integer) Inkrementierung

`increment_schueler_year_fields(db, do_commit=True)`: Erhöht Integer-Jahresfelder um 1.

**Betroffene Spalten:**
- JahrZuzug, JahrWechsel_SI, JahrWechsel_SII

**Eigenschaften:**
- NULL-sicher (NULL bleibt NULL)
- Ein Bulk-Update für alle drei Felder

### SchuelerAbgaenge-Datumsfelder-Inkrementierung

`increment_schueler_abgaenge_dates(db, do_commit=True)`: Erhöht Datumsfelder in der Tabelle SchuelerAbgaenge um 1 Jahr.

**Betroffene Spalten:**
- LSSchulEntlassDatum
- LSBeginnDatum

**Eigenschaften:**
- Nutzt `DATE_ADD` für korrekte Jahresverschiebung
- NULL-sicher (NULL bleibt NULL)
- Ein Bulk-Update für beide Felder mit Sample-Logging

### SchuelerLernabschnittsdaten-Datumsfelder-Inkrementierung

`increment_schueler_lernabschnittsdaten_dates(db, do_commit=True)`: Erhöht Datumsfelder in der Tabelle SchuelerLernabschnittsdaten um 1 Jahr.

**Betroffene Spalten:**
- DatumVon, DatumBis
- Konferenzdatum, Zeugnisdatum
- NPV_Datum, NPAA_Datum, NPBQ_Datum, DatumFHR

**Eigenschaften:**
- Nutzt `DATE_ADD` für korrekte Jahresverschiebung
- NULL-sicher (NULL bleibt NULL)
- Bulk-Update für alle acht Felder mit Sample-Logging

### SchuelerLeistungsdaten – Warndatum (Date) Inkrementierung

`increment_schueler_leistungsdaten_warndatum(db, do_commit=True)`: Erhöht das Feld `Warndatum` in der Tabelle `SchuelerLeistungsdaten` um 1 Jahr (nur wenn nicht NULL).

### Orchestrator & Dry-Run

`run_all_increments(config_path="config.json", dry_run=False, steps=None)`: Führt alle Schritte in einer orchestrierten Transaktion aus.

**Eigenschaften:**
- Führt die Schritte in sinnvoller Reihenfolge aus und nutzt eine gemeinsame Transaktion
- Setzt Autocommit intern außer Kraft und steuert Commit/Rollback zentral
- `dry_run=True`: Alle Änderungen werden durchgeführt, am Ende jedoch zurückgerollt
- `steps`: Optional Liste von Schritten (Keys) zur gezielten Ausführung, z.B. `["schueler_dates", "schueler_abschlussdatum"]`

**Verfügbare Schritt-Keys:**
- `schuljahresabschnitte`
- `schueler_dates`
- `schueler_abschlussdatum`
- `schueler_year_fields`
- `schueler_abgaenge_dates`
- `schueler_lernabschnittsdaten_dates`
- `schueler_leistungsdaten_warndatum`

**Beispiel (Dry-Run):**

```python
from svws_schuljahr import run_all_increments

# Führe alle Schritte aus, ohne zu committen (Rollback am Ende)
ok = run_all_increments("config.json", dry_run=True)
print("OK" if ok else "Fehler")
```

**Beispiel (Nur bestimmte Schritte, Commit):**

```python
from svws_schuljahr import run_all_increments

# Nur Schüler-Daten und Abschlussdatum mit Commit
ok = run_all_increments(
  "config.json",
  dry_run=False,
  steps=["schueler_dates", "schueler_abschlussdatum"]
)
print("OK" if ok else "Fehler")
```

**Eigenschaften:**
- Nutzt `DATE_ADD(Warndatum, INTERVAL 1 YEAR)`
- NULL-sicher (NULL bleibt NULL)
- Liefert Beispielwerte im Log zur Verifikation

## Logging

Das Tool verwendet Python-Logging und gibt detaillierte Informationen aus:

- `INFO`: Erfolgreiche Operationen (Verbindungen, Updates)
- `WARNING`: Potenzielle Probleme (z.B. existierende Jahre)
- `ERROR`: Fehlerhafte Operationen

Die Logging-Konfiguration kann in `config.json` angepasst werden.

## Beispiel-Output

```
INFO:__main__:Configuration loaded from config.json
INFO:__main__:Successfully connected to MariaDB at localhost:3306/anonym
INFO:__main__:Found 10 distinct years: [2030, 2029, 2028, 2027, 2026, 2025, 2024, 2023, 2022, 2021]
INFO:__main__:Updated 1 rows: Jahr 2030 -> 2031
INFO:__main__:Updated 2 rows: Jahr 2029 -> 2030
...
INFO:__main__:Successfully incremented all years in Schuljahresabschnitte
✓ Years successfully incremented
```

## Sicherheitsmerkmale

- **Transaktionsmanagement**: Alle Änderungen werden in Transaktionen ausgeführt
- **Parametrisierte Queries**: Schutz vor SQL-Injection
- **Fehlerbehandlung**: Automatisches Rollback bei Fehlern
- **Constraint-Validierung**: Prüfung auf eindeutige Constraint-Verletzungen
- **Sichere Verbindung**: Unterstützung für verschlüsselte Verbindungen

## Fehlerbehandlung

Das Tool implementiert umfassende Fehlerbehandlung:

- Verbindungsfehler werden geloggt und behandelt
- Query-Fehler werden mit Logging dokumentiert
- Transaktionsfehler führen zu automatischem Rollback
- Detaillierte Error-Messages unterstützen die Fehlerbehebung

## Lizenz

Siehe LICENSE-Datei
