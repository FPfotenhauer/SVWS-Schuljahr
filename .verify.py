import sys
import svws_schuljahr as m

db = m.create_connection_from_config('config.json')
if not db or not db.connect():
    print('CONNECT_FAILED')
    sys.exit(1)

try:
    years = db.execute_query("SELECT DISTINCT Jahr FROM Schuljahresabschnitte ORDER BY Jahr DESC")
    print('YEARS_DESC=', [r[0] for r in years])

    schueler = db.execute_query("SELECT ID, Geburtsdatum, Schulwechseldatum FROM Schueler LIMIT 5")
    print('SCHUELER_SAMPLE=', schueler)

    abschluss = db.execute_query("SELECT ID, Abschlussdatum FROM Schueler WHERE Abschlussdatum IS NOT NULL AND Abschlussdatum <> '' LIMIT 5")
    print('ABSCHLUSS_SAMPLE=', abschluss)

    warndatum_count = db.execute_query("SELECT COUNT(*) FROM SchuelerLeistungsdaten WHERE Warndatum IS NOT NULL")
    print('WARNDATUM_COUNT=', warndatum_count[0][0] if warndatum_count else None)
finally:
    db.disconnect()
