#!/usr/bin/env python3
"""
MariaDB Database Connection Module
Provides connection management for MariaDB 11+ databases
"""

import os
import sys
import json
import argparse
from typing import Optional, Dict, Any, List, Tuple
import logging
from pathlib import Path

try:
    import mariadb
except ImportError:
    print("Error: mariadb Python package not installed.")
    print("Install it with: pip install mariadb")
    sys.exit(1)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.json") -> Dict[str, Any]:
    """
    Load configuration from a JSON file.

    Args:
        config_path: Path to the config.json file (default: config.json)

    Returns:
        Configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        raise


class MariaDBConnection:
    """
    Manages MariaDB 11+ database connections with connection pooling support.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "SVWS",
        autocommit: bool = True,
    ):
        """
        Initialize MariaDB connection parameters.

        Args:
            host: Database server hostname (default: localhost)
            port: Database server port (default: 3306)
            user: Database user (default: root)
            password: Database password (default: empty)
            database: Database name (default: SVWS)
            autocommit: Enable autocommit mode (default: True)
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.autocommit = autocommit
        self.connection: Optional[mariadb.Connection] = None

    def connect(self) -> bool:
        """
        Establish a connection to the MariaDB database.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.connection = mariadb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=self.autocommit,
            )
            logger.info(
                f"Successfully connected to MariaDB at {self.host}:{self.port}/{self.database}"
            )
            return True
        except mariadb.Error as e:
            logger.error(f"Failed to connect to MariaDB: {e}")
            return False

    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

    def execute_query(self, query: str, params: tuple = ()) -> Optional[list]:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query string
            params: Query parameters for parameterized queries (tuple)

        Returns:
            List of results or None if error occurs
        """
        if not self.connection:
            logger.error("No active database connection")
            return None

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            return results
        except mariadb.Error as e:
            logger.error(f"Query execution failed: {e}")
            return None

    def execute_update(self, query: str, params: tuple = ()) -> Optional[int]:
        """
        Execute an INSERT, UPDATE, or DELETE query.

        Args:
            query: SQL query string
            params: Query parameters for parameterized queries (tuple)

        Returns:
            Number of affected rows or None if error occurs
        """
        if not self.connection:
            logger.error("No active database connection")
            return None

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            affected_rows = cursor.rowcount
            cursor.close()
            return affected_rows
        except mariadb.Error as e:
            logger.error(f"Update execution failed: {e}")
            return None

    def commit(self) -> bool:
        """
        Commit pending transactions.

        Returns:
            True if successful, False otherwise
        """
        if not self.connection:
            logger.error("No active database connection")
            return False

        try:
            self.connection.commit()
            logger.info("Transaction committed")
            return True
        except mariadb.Error as e:
            logger.error(f"Commit failed: {e}")
            return False

    def rollback(self) -> bool:
        """
        Rollback pending transactions.

        Returns:
            True if successful, False otherwise
        """
        if not self.connection:
            logger.error("No active database connection")
            return False

        try:
            self.connection.rollback()
            logger.info("Transaction rolled back")
            return True
        except mariadb.Error as e:
            logger.error(f"Rollback failed: {e}")
            return False

    def get_cursor(self):
        """Get a database cursor for direct execution."""
        if not self.connection:
            logger.error("No active database connection")
            return None
        return self.connection.cursor()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


def create_connection_from_config(config_path: str = "config.json") -> Optional[MariaDBConnection]:
    """
    Create a MariaDB connection from a config.json file.

    Args:
        config_path: Path to the config.json file (default: config.json)

    Returns:
        MariaDBConnection instance or None if config loading fails
    """
    try:
        config = load_config(config_path)
        db_config = config.get("database", {})
        
        return MariaDBConnection(
            host=db_config.get("host", "localhost"),
            port=db_config.get("port", 3306),
            user=db_config.get("user", "root"),
            password=db_config.get("password", ""),
            database=db_config.get("database", "SVWS"),
            autocommit=db_config.get("autocommit", True),
        )
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to create connection from config: {e}")
        return None


def create_connection_from_env() -> Optional[MariaDBConnection]:
    """
    Create a MariaDB connection using environment variables.

    Expected environment variables:
        MARIADB_HOST: Database host
        MARIADB_PORT: Database port
        MARIADB_USER: Database user
        MARIADB_PASSWORD: Database password
        MARIADB_DATABASE: Database name

    Returns:
        MariaDBConnection instance or None if required variables are missing
    """
    required_vars = ["MARIADB_HOST", "MARIADB_USER", "MARIADB_PASSWORD", "MARIADB_DATABASE"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return None

    return MariaDBConnection(
        host=os.getenv("MARIADB_HOST", "localhost"),
        port=int(os.getenv("MARIADB_PORT", "3306")),
        user=os.getenv("MARIADB_USER"),
        password=os.getenv("MARIADB_PASSWORD"),
        database=os.getenv("MARIADB_DATABASE"),
    )


def increment_schuljahresabschnitte_jahr(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment the Jahr (year) column in Schuljahresabschnitte table by 1.
    Processes years in descending order to avoid unique constraint violations.

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    try:
        # Get all distinct Jahre (years) from the table, ordered descending
        distinct_jahre_query = "SELECT DISTINCT Jahr FROM Schuljahresabschnitte ORDER BY Jahr DESC"
        jahre_result = db.execute_query(distinct_jahre_query)
        
        if not jahre_result:
            logger.warning("No years found in Schuljahresabschnitte table")
            return True
        
        jahre_list = [row[0] for row in jahre_result]
        logger.info(f"Found {len(jahre_list)} distinct years: {jahre_list}")
        
        # Update each year, starting from the highest
        for jahr in jahre_list:
            new_jahr = jahr + 1
            
            # Check if new_jahr already exists to avoid constraint violations
            check_query = "SELECT COUNT(*) FROM Schuljahresabschnitte WHERE Jahr = %s"
            check_result = db.execute_query(check_query, (new_jahr,))
            
            if check_result and check_result[0][0] > 0:
                logger.warning(f"Year {new_jahr} already exists in table, skipping update for {jahr}")
                continue
            
            # Update the year
            update_query = "UPDATE Schuljahresabschnitte SET Jahr = %s WHERE Jahr = %s"
            affected_rows = db.execute_update(update_query, (new_jahr, jahr))
            
            if affected_rows is not None:
                logger.info(f"Updated {affected_rows} rows: Jahr {jahr} -> {new_jahr}")
            else:
                logger.error(f"Failed to update Jahr {jahr}")
                return False
        
        # Commit the transaction
        if do_commit:
            if db.commit():
                logger.info("Successfully incremented all years in Schuljahresabschnitte")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True
            
    except Exception as e:
        logger.error(f"Error incrementing Schuljahresabschnitte: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False


def increment_schueler_fehlstunden_datum(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment the Datum field in the SchuelerFehlstunden table by 1 year (if not NULL).

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    try:
        count_query = "SELECT COUNT(*) FROM SchuelerFehlstunden"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} records in SchuelerFehlstunden table")

        update_query = (
            "UPDATE SchuelerFehlstunden "
            "SET Datum = IF(Datum IS NOT NULL, DATE_ADD(Datum, INTERVAL 1 YEAR), NULL)"
        )
        logger.info("Incrementing SchuelerFehlstunden.Datum by 1 year where not NULL")

        affected_rows = db.execute_update(update_query)

        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} SchuelerFehlstunden records")

            sample_query = (
                "SELECT ID, Datum "
                "FROM SchuelerFehlstunden "
                "WHERE Datum IS NOT NULL "
                "LIMIT 5"
            )
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated SchuelerFehlstunden:")
                for row in sample_result:
                    logger.info(f"  ID: {row[0]}, Datum: {row[1]}")
        else:
            logger.error("Failed to update Datum in SchuelerFehlstunden")
            return False

        if do_commit:
            if db.commit():
                logger.info("Successfully incremented Datum in SchuelerFehlstunden table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True

    except Exception as e:
        logger.error(f"Error incrementing SchuelerFehlstunden Datum: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False

def increment_schueler_dates(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment all date fields in the Schueler table by 1 year.
    
    Updates the following date columns:
    - Geburtsdatum
    - Religionsabmeldung
    - Religionsanmeldung
    - Schulwechseldatum
    - BeginnBildungsgang
    - AnmeldeDatum
    - EndeEingliederung
    - EndeAnschlussfoerderung
    - SprachfoerderungVon
    - SprachfoerderungBis

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    # Define all date fields to increment
    date_fields = [
        "Geburtsdatum",
        "Religionsabmeldung",
        "Religionsanmeldung",
        "Schulwechseldatum",
        "BeginnBildungsgang",
        "AnmeldeDatum",
        "EndeEingliederung",
        "EndeAnschlussfoerderung",
        "SprachfoerderungVon",
        "SprachfoerderungBis"
    ]

    try:
        # Count total records
        count_query = "SELECT COUNT(*) FROM Schueler"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} student records in Schueler table")

        # Build the UPDATE statement with DATE_ADD for each field
        # Only update non-NULL values
        set_clauses = [
            f"{field} = IF({field} IS NOT NULL, DATE_ADD({field}, INTERVAL 1 YEAR), NULL)"
            for field in date_fields
        ]
        
        update_query = f"UPDATE Schueler SET {', '.join(set_clauses)}"
        
        logger.info(f"Incrementing {len(date_fields)} date fields by 1 year")
        
        # Execute the update
        affected_rows = db.execute_update(update_query)
        
        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} student records")
            
            # Log sample of updated dates for verification
            sample_query = """
                SELECT ID, Geburtsdatum, Schulwechseldatum, AnmeldeDatum 
                FROM Schueler 
                LIMIT 3
            """
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated records:")
                for row in sample_result:
                    logger.info(f"  ID: {row[0]}, Geburtsdatum: {row[1]}, Schulwechseldatum: {row[2]}, AnmeldeDatum: {row[3]}")
        else:
            logger.error("Failed to update Schueler date fields")
            return False
        
        # Commit the transaction
        if do_commit:
            if db.commit():
                logger.info("Successfully incremented all date fields in Schueler table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True
            
    except Exception as e:
        logger.error(f"Error incrementing Schueler dates: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False


def increment_schueler_vermerke_datum(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment the Datum field in the SchuelerVermerke table by 1 year (if not NULL).

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    try:
        count_query = "SELECT COUNT(*) FROM SchuelerVermerke"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} records in SchuelerVermerke table")

        update_query = (
            "UPDATE SchuelerVermerke "
            "SET Datum = IF(Datum IS NOT NULL, DATE_ADD(Datum, INTERVAL 1 YEAR), NULL)"
        )
        logger.info("Incrementing SchuelerVermerke.Datum by 1 year where not NULL")

        affected_rows = db.execute_update(update_query)

        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} SchuelerVermerke records")

            sample_query = (
                "SELECT ID, Datum "
                "FROM SchuelerVermerke "
                "WHERE Datum IS NOT NULL "
                "LIMIT 5"
            )
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated SchuelerVermerke:")
                for row in sample_result:
                    logger.info(f"  ID: {row[0]}, Datum: {row[1]}")
        else:
            logger.error("Failed to update Datum in SchuelerVermerke")
            return False

        if do_commit:
            if db.commit():
                logger.info("Successfully incremented Datum in SchuelerVermerke table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True

    except Exception as e:
        logger.error(f"Error incrementing SchuelerVermerke Datum: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False

def increment_schueler_abschlussdatum(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment the Abschlussdatum field in Schueler table by 1 year.
    
    Abschlussdatum is stored as VARCHAR(15) in German date format (DD.MM.YYYY or D.M.YYYY).
    This function parses the string, adds 1 year, and formats it back to the same format.

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    try:
        # Count records with Abschlussdatum
        count_query = "SELECT COUNT(*) FROM Schueler WHERE Abschlussdatum IS NOT NULL AND Abschlussdatum != ''"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} student records with Abschlussdatum")

        if total_records == 0:
            logger.info("No Abschlussdatum values to update")
            return True

        # Update Abschlussdatum by parsing the German date format
        # The format is D.M.YYYY or DD.MM.YYYY
        # We use STR_TO_DATE to parse it, add 1 year, then format it back
        # Use CONCAT to build the format dynamically to preserve single-digit days/months
        update_query = """
            UPDATE Schueler 
            SET Abschlussdatum = CONCAT(
                DAY(DATE_ADD(STR_TO_DATE(Abschlussdatum, '%d.%m.%Y'), INTERVAL 1 YEAR)),
                '.',
                MONTH(DATE_ADD(STR_TO_DATE(Abschlussdatum, '%d.%m.%Y'), INTERVAL 1 YEAR)),
                '.',
                YEAR(DATE_ADD(STR_TO_DATE(Abschlussdatum, '%d.%m.%Y'), INTERVAL 1 YEAR))
            )
            WHERE Abschlussdatum IS NOT NULL 
            AND Abschlussdatum != ''
            AND Abschlussdatum REGEXP '^[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{4}$'
        """
        
        logger.info("Incrementing Abschlussdatum field by 1 year")
        
        # Execute the update
        affected_rows = db.execute_update(update_query)
        
        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} Abschlussdatum records")
            
            # Log sample of updated dates for verification
            sample_query = """
                SELECT ID, Abschlussdatum 
                FROM Schueler 
                WHERE Abschlussdatum IS NOT NULL AND Abschlussdatum != ''
                LIMIT 5
            """
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated Abschlussdatum values:")
                for row in sample_result:
                    logger.info(f"  ID: {row[0]}, Abschlussdatum: {row[1]}")
        else:
            logger.error("Failed to update Abschlussdatum field")
            return False
        
        # Commit the transaction
        if do_commit:
            if db.commit():
                logger.info("Successfully incremented Abschlussdatum in Schueler table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True
            
    except Exception as e:
        logger.error(f"Error incrementing Abschlussdatum: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False


def increment_schueler_merkmale_dates(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment the DatumVon and DatumBis fields in the SchuelerMerkmale table by 1 year (if not NULL).

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    date_fields = ["DatumVon", "DatumBis"]

    try:
        count_query = "SELECT COUNT(*) FROM SchuelerMerkmale"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} records in SchuelerMerkmale table")

        set_clauses = [
            f"{field} = IF({field} IS NOT NULL, DATE_ADD({field}, INTERVAL 1 YEAR), NULL)"
            for field in date_fields
        ]

        update_query = f"UPDATE SchuelerMerkmale SET {', '.join(set_clauses)}"
        logger.info("Incrementing SchuelerMerkmale.DatumVon/DatumBis by 1 year where not NULL")

        affected_rows = db.execute_update(update_query)

        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} SchuelerMerkmale records")

            sample_query = (
                "SELECT ID, DatumVon, DatumBis "
                "FROM SchuelerMerkmale "
                "WHERE DatumVon IS NOT NULL OR DatumBis IS NOT NULL "
                "LIMIT 5"
            )
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated SchuelerMerkmale:")
                for row in sample_result:
                    logger.info(f"  ID: {row[0]}, DatumVon: {row[1]}, DatumBis: {row[2]}")
        else:
            logger.error("Failed to update dates in SchuelerMerkmale")
            return False

        if do_commit:
            if db.commit():
                logger.info("Successfully incremented dates in SchuelerMerkmale table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True

    except Exception as e:
        logger.error(f"Error incrementing SchuelerMerkmale dates: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False

def increment_schueler_year_fields(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment integer year fields in the Schueler table by 1.
    
    Updates the following integer year columns:
    - JahrZuzug
    - JahrWechsel_SI
    - JahrWechsel_SII

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    # Define all year fields to increment
    year_fields = [
        "JahrZuzug",
        "JahrWechsel_SI",
        "JahrWechsel_SII"
    ]

    try:
        # Count total records
        count_query = "SELECT COUNT(*) FROM Schueler"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} student records in Schueler table")

        # Build the UPDATE statement for integer year fields
        # Only update non-NULL values
        set_clauses = [
            f"{field} = IF({field} IS NOT NULL, {field} + 1, NULL)"
            for field in year_fields
        ]
        
        update_query = f"UPDATE Schueler SET {', '.join(set_clauses)}"
        
        logger.info(f"Incrementing {len(year_fields)} year fields by 1")
        
        # Execute the update
        affected_rows = db.execute_update(update_query)
        
        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} student records")
            
            # Log sample of updated years for verification
            sample_query = """
                SELECT ID, JahrZuzug, JahrWechsel_SI, JahrWechsel_SII 
                FROM Schueler 
                WHERE JahrZuzug IS NOT NULL OR JahrWechsel_SI IS NOT NULL OR JahrWechsel_SII IS NOT NULL
                LIMIT 5
            """
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated records:")
                for row in sample_result:
                    logger.info(f"  ID: {row[0]}, JahrZuzug: {row[1]}, JahrWechsel_SI: {row[2]}, JahrWechsel_SII: {row[3]}")
        else:
            logger.error("Failed to update Schueler year fields")
            return False
        
        # Commit the transaction
        if do_commit:
            if db.commit():
                logger.info("Successfully incremented all year fields in Schueler table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True
            
    except Exception as e:
        logger.error(f"Error incrementing Schueler year fields: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False


def increment_schueler_foerderempfehlungen_dates(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment date fields in the SchuelerFoerderempfehlungen table by 1 year (if not NULL).

    Fields:
    - DatumAngelegt
    - Zeitrahmen_von_Datum
    - Zeitrahmen_bis_Datum
    - Ueberpruefung_Datum
    - Naechstes_Beratungsgespraech

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    date_fields = [
        "DatumAngelegt",
        "Zeitrahmen_von_Datum",
        "Zeitrahmen_bis_Datum",
        "Ueberpruefung_Datum",
        "Naechstes_Beratungsgespraech",
    ]

    try:
        count_query = "SELECT COUNT(*) FROM SchuelerFoerderempfehlungen"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} records in SchuelerFoerderempfehlungen table")

        set_clauses = [
            f"{field} = IF({field} IS NOT NULL, DATE_ADD({field}, INTERVAL 1 YEAR), NULL)"
            for field in date_fields
        ]

        update_query = f"UPDATE SchuelerFoerderempfehlungen SET {', '.join(set_clauses)}"
        logger.info("Incrementing SchuelerFoerderempfehlungen date fields by 1 year where not NULL")

        affected_rows = db.execute_update(update_query)

        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} SchuelerFoerderempfehlungen records")

            sample_query = (
                "SELECT DatumAngelegt, Zeitrahmen_von_Datum, Zeitrahmen_bis_Datum, Ueberpruefung_Datum, Naechstes_Beratungsgespraech "
                "FROM SchuelerFoerderempfehlungen "
                "WHERE DatumAngelegt IS NOT NULL OR Zeitrahmen_von_Datum IS NOT NULL OR Zeitrahmen_bis_Datum IS NOT NULL OR Ueberpruefung_Datum IS NOT NULL OR Naechstes_Beratungsgespraech IS NOT NULL "
                "LIMIT 5"
            )
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated SchuelerFoerderempfehlungen:")
                for row in sample_result:
                    logger.info(
                        f"  DatumAngelegt: {row[0]}, Zeitrahmen_von_Datum: {row[1]}, Zeitrahmen_bis_Datum: {row[2]}, Ueberpruefung_Datum: {row[3]}, Naechstes_Beratungsgespraech: {row[4]}"
                    )
        else:
            logger.error("Failed to update dates in SchuelerFoerderempfehlungen")
            return False

        if do_commit:
            if db.commit():
                logger.info("Successfully incremented dates in SchuelerFoerderempfehlungen table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True

    except Exception as e:
        logger.error(f"Error incrementing SchuelerFoerderempfehlungen dates: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False

def increment_schueler_abgaenge_dates(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment date fields in the SchuelerAbgaenge table by 1 year.

    Updates the following date columns:
    - LSSchulEntlassDatum
    - LSBeginnDatum

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    date_fields = [
        "LSSchulEntlassDatum",
        "LSBeginnDatum",
    ]

    try:
        count_query = "SELECT COUNT(*) FROM SchuelerAbgaenge"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} records in SchuelerAbgaenge table")

        set_clauses = [
            f"{field} = IF({field} IS NOT NULL, DATE_ADD({field}, INTERVAL 1 YEAR), NULL)"
            for field in date_fields
        ]

        update_query = f"UPDATE SchuelerAbgaenge SET {', '.join(set_clauses)}"
        logger.info(f"Incrementing {len(date_fields)} SchuelerAbgaenge date fields by 1 year")

        affected_rows = db.execute_update(update_query)

        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} SchuelerAbgaenge records")

            sample_query = """
                SELECT ID, LSSchulEntlassDatum, LSBeginnDatum
                FROM SchuelerAbgaenge
                LIMIT 5
            """
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated SchuelerAbgaenge records:")
                for row in sample_result:
                    logger.info(
                        f"  ID: {row[0]}, LSSchulEntlassDatum: {row[1]}, LSBeginnDatum: {row[2]}"
                    )
        else:
            logger.error("Failed to update SchuelerAbgaenge date fields")
            return False

        if do_commit:
            if db.commit():
                logger.info("Successfully incremented date fields in SchuelerAbgaenge table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True

    except Exception as e:
        logger.error(f"Error incrementing SchuelerAbgaenge dates: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False


def increment_schueler_allgadr_dates(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment the Vertragsbeginn and Vertragsende fields in the Schueler_AllgAdr table by 1 year (if not NULL).

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    date_fields = ["Vertragsbeginn", "Vertragsende"]

    try:
        count_query = "SELECT COUNT(*) FROM Schueler_AllgAdr"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} records in Schueler_AllgAdr table")

        set_clauses = [
            f"{field} = IF({field} IS NOT NULL, DATE_ADD({field}, INTERVAL 1 YEAR), NULL)"
            for field in date_fields
        ]

        update_query = f"UPDATE Schueler_AllgAdr SET {', '.join(set_clauses)}"
        logger.info("Incrementing Schueler_AllgAdr.Vertragsbeginn/Vertragsende by 1 year where not NULL")

        affected_rows = db.execute_update(update_query)

        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} Schueler_AllgAdr records")

            sample_query = (
                "SELECT ID, Vertragsbeginn, Vertragsende "
                "FROM Schueler_AllgAdr "
                "WHERE Vertragsbeginn IS NOT NULL OR Vertragsende IS NOT NULL "
                "LIMIT 5"
            )
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated Schueler_AllgAdr:")
                for row in sample_result:
                    logger.info(f"  ID: {row[0]}, Vertragsbeginn: {row[1]}, Vertragsende: {row[2]}")
        else:
            logger.error("Failed to update dates in Schueler_AllgAdr")
            return False

        if do_commit:
            if db.commit():
                logger.info("Successfully incremented dates in Schueler_AllgAdr table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True

    except Exception as e:
        logger.error(f"Error incrementing Schueler_AllgAdr dates: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False

def increment_schueler_lernabschnittsdaten_dates(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment date fields in the SchuelerLernabschnittsdaten table by 1 year.

    Updates the following date columns:
    - DatumVon
    - DatumBis
    - Konferenzdatum
    - Zeugnisdatum
    - NPV_Datum
    - NPAA_Datum
    - NPBQ_Datum
    - DatumFHR

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    date_fields = [
        "DatumVon",
        "DatumBis",
        "Konferenzdatum",
        "Zeugnisdatum",
        "NPV_Datum",
        "NPAA_Datum",
        "NPBQ_Datum",
        "DatumFHR",
    ]

    try:
        count_query = "SELECT COUNT(*) FROM SchuelerLernabschnittsdaten"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} records in SchuelerLernabschnittsdaten table")

        set_clauses = [
            f"{field} = IF({field} IS NOT NULL, DATE_ADD({field}, INTERVAL 1 YEAR), NULL)"
            for field in date_fields
        ]

        update_query = f"UPDATE SchuelerLernabschnittsdaten SET {', '.join(set_clauses)}"
        logger.info(f"Incrementing {len(date_fields)} SchuelerLernabschnittsdaten date fields by 1 year")

        affected_rows = db.execute_update(update_query)

        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} SchuelerLernabschnittsdaten records")

            sample_query = """
                SELECT ID, DatumVon, DatumBis, Zeugnisdatum
                FROM SchuelerLernabschnittsdaten
                LIMIT 5
            """
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated SchuelerLernabschnittsdaten records:")
                for row in sample_result:
                    logger.info(
                        f"  ID: {row[0]}, DatumVon: {row[1]}, DatumBis: {row[2]}, Zeugnisdatum: {row[3]}"
                    )
        else:
            logger.error("Failed to update SchuelerLernabschnittsdaten date fields")
            return False

        if do_commit:
            if db.commit():
                logger.info("Successfully incremented date fields in SchuelerLernabschnittsdaten table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True

    except Exception as e:
        logger.error(f"Error incrementing SchuelerLernabschnittsdaten dates: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False


def increment_schueler_leistungsdaten_warndatum(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment the Warndatum field in the SchuelerLeistungsdaten table by 1 year (if not NULL).

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    try:
        count_query = "SELECT COUNT(*) FROM SchuelerLeistungsdaten"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} records in SchuelerLeistungsdaten table")

        update_query = (
            "UPDATE SchuelerLeistungsdaten "
            "SET Warndatum = IF(Warndatum IS NOT NULL, DATE_ADD(Warndatum, INTERVAL 1 YEAR), NULL)"
        )
        logger.info("Incrementing Warndatum by 1 year where not NULL")

        affected_rows = db.execute_update(update_query)

        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} SchuelerLeistungsdaten records")

            sample_query = """
                SELECT ID, Warndatum
                FROM SchuelerLeistungsdaten
                WHERE Warndatum IS NOT NULL
                LIMIT 5
            """
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated SchuelerLeistungsdaten:")
                for row in sample_result:
                    logger.info(f"  ID: {row[0]}, Warndatum: {row[1]}")
        else:
            logger.error("Failed to update Warndatum in SchuelerLeistungsdaten")
            return False

        if do_commit:
            if db.commit():
                logger.info("Successfully incremented Warndatum in SchuelerLeistungsdaten table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True

    except Exception as e:
        logger.error(f"Error incrementing SchuelerLeistungsdaten Warndatum: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False


def increment_schueler_einzelleistungen_datum(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment the Datum field in the SchuelerEinzelleistungen table by 1 year (if not NULL).

    Args:
        db: MariaDBConnection instance (must be connected)

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    try:
        count_query = "SELECT COUNT(*) FROM SchuelerEinzelleistungen"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} records in SchuelerEinzelleistungen table")

        update_query = (
            "UPDATE SchuelerEinzelleistungen "
            "SET Datum = IF(Datum IS NOT NULL, DATE_ADD(Datum, INTERVAL 1 YEAR), NULL)"
        )
        logger.info("Incrementing SchuelerEinzelleistungen.Datum by 1 year where not NULL")

        affected_rows = db.execute_update(update_query)

        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} SchuelerEinzelleistungen records")

            sample_query = (
                "SELECT ID, Datum "
                "FROM SchuelerEinzelleistungen "
                "WHERE Datum IS NOT NULL "
                "LIMIT 5"
            )
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated SchuelerEinzelleistungen:")
                for row in sample_result:
                    logger.info(f"  ID: {row[0]}, Datum: {row[1]}")
        else:
            logger.error("Failed to update Datum in SchuelerEinzelleistungen")
            return False

        if do_commit:
            if db.commit():
                logger.info("Successfully incremented Datum in SchuelerEinzelleistungen table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True

    except Exception as e:
        logger.error(f"Error incrementing SchuelerEinzelleistungen Datum: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False


def increment_lehrer_dates(db: MariaDBConnection, do_commit: bool = True) -> bool:
    """
    Increment date fields in the K_Lehrer table by 1 year (if not NULL).
    
    Fields incremented:
    - Geburtsdatum
    - DatumZugang
    - DatumAbgang

    Args:
        db: MariaDBConnection instance (must be connected)
        do_commit: If True, commits the transaction; if False, leaves it open for orchestrator

    Returns:
        True if successful, False otherwise
    """
    if not db or not db.connection:
        logger.error("No active database connection")
        return False

    try:
        count_query = "SELECT COUNT(*) FROM K_Lehrer"
        count_result = db.execute_query(count_query)
        total_records = count_result[0][0] if count_result else 0
        logger.info(f"Processing {total_records} records in K_Lehrer table")

        update_query = (
            "UPDATE K_Lehrer "
            "SET "
            "Geburtsdatum = IF(Geburtsdatum IS NOT NULL, DATE_ADD(Geburtsdatum, INTERVAL 1 YEAR), NULL), "
            "DatumZugang = IF(DatumZugang IS NOT NULL, DATE_ADD(DatumZugang, INTERVAL 1 YEAR), NULL), "
            "DatumAbgang = IF(DatumAbgang IS NOT NULL, DATE_ADD(DatumAbgang, INTERVAL 1 YEAR), NULL)"
        )
        logger.info("Incrementing K_Lehrer date fields (Geburtsdatum, DatumZugang, DatumAbgang) by 1 year where not NULL")

        affected_rows = db.execute_update(update_query)

        if affected_rows is not None:
            logger.info(f"Updated {affected_rows} K_Lehrer records")

            sample_query = """
                SELECT ID, Geburtsdatum, DatumZugang, DatumAbgang
                FROM K_Lehrer
                WHERE Geburtsdatum IS NOT NULL OR DatumZugang IS NOT NULL OR DatumAbgang IS NOT NULL
                LIMIT 5
            """
            sample_result = db.execute_query(sample_query)
            if sample_result:
                logger.info("Sample of updated K_Lehrer records:")
                for row in sample_result:
                    logger.info(f"  ID: {row[0]}, Geburtsdatum: {row[1]}, DatumZugang: {row[2]}, DatumAbgang: {row[3]}")
        else:
            logger.error("Failed to update date fields in K_Lehrer")
            return False

        if do_commit:
            if db.commit():
                logger.info("Successfully incremented date fields in K_Lehrer table")
                return True
            else:
                logger.error("Failed to commit transaction")
                return False
        else:
            logger.info("Skipping commit (dry-run mode)")
            return True

    except Exception as e:
        logger.error(f"Error incrementing K_Lehrer dates: {e}")
        try:
            db.rollback()
            logger.info("Transaction rolled back")
        except Exception as rollback_error:
            logger.error(f"Failed to rollback transaction: {rollback_error}")
        return False


def get_available_steps() -> List[Tuple[str, Any]]:
    """
    Return the list of available orchestrated steps as (key, function) tuples.
    """
    return [
        ("schuljahresabschnitte", increment_schuljahresabschnitte_jahr),
        ("schueler_dates", increment_schueler_dates),
        ("schueler_abschlussdatum", increment_schueler_abschlussdatum),
        ("schueler_year_fields", increment_schueler_year_fields),
        ("schueler_abgaenge_dates", increment_schueler_abgaenge_dates),
        ("schueler_lernabschnittsdaten_dates", increment_schueler_lernabschnittsdaten_dates),
        ("schueler_leistungsdaten_warndatum", increment_schueler_leistungsdaten_warndatum),
        ("schueler_einzelleistungen_datum", increment_schueler_einzelleistungen_datum),
        ("schueler_fehlstunden_datum", increment_schueler_fehlstunden_datum),
        ("schueler_vermerke_datum", increment_schueler_vermerke_datum),
        ("schueler_merkmale_dates", increment_schueler_merkmale_dates),
        ("schueler_foerderempfehlungen_dates", increment_schueler_foerderempfehlungen_dates),
        ("schueler_allgadr_dates", increment_schueler_allgadr_dates),
        ("lehrer_dates", increment_lehrer_dates),
    ]


def run_all_increments(
    config_path: str = "config.json",
    dry_run: bool = False,
    steps: Optional[list] = None,
) -> bool:
    """
    Run all increment steps in a single orchestrated transaction.

    Args:
        config_path: Path to config.json
        dry_run: If True, performs all updates but rolls back at the end
        steps: Optional list of step keys to execute in order. If None, runs all.

    Returns:
        True if all selected steps succeed (and committed unless dry_run), False otherwise
    """
    # Define available steps in execution order
    available_steps = get_available_steps()

    # Build the final execution list
    if steps is None:
        exec_steps = available_steps
    else:
        allowed = dict(available_steps)
        exec_steps = []
        for key in steps:
            func = allowed.get(key)
            if not func:
                logger.error(f"Unknown step '{key}'. Allowed: {list(allowed.keys())}")
                return False
            exec_steps.append((key, func))

    # Create connection with autocommit disabled for transactional control
    db = create_connection_from_config(config_path)
    if not db:
        logger.error("Failed to load DB configuration")
        return False
    # Ensure autocommit is disabled during orchestration
    db.autocommit = False
    if not db.connect():
        logger.error("Failed to connect to database")
        return False

    # Double-ensure autocommit disabled on the underlying connection
    try:
        if hasattr(db, "connection") and db.connection:
            try:
                db.connection.autocommit = False
            except Exception:
                pass
    except Exception:
        pass

    try:
        logger.info(
            f"Starting orchestrated run with steps: {[key for key, _ in exec_steps]} (dry_run={dry_run})"
        )

        # Execute each step without committing individually
        for key, func in exec_steps:
            logger.info(f"Running step: {key}")
            ok = func(db, do_commit=False)
            if not ok:
                logger.error(f"Step failed: {key}. Rolling back.")
                db.rollback()
                return False

        # All steps succeeded
        if dry_run:
            logger.info("Dry-run enabled; rolling back all changes.")
            db.rollback()
            return True
        else:
            if db.commit():
                logger.info("All steps completed and committed successfully.")
                return True
            else:
                logger.error("Final commit failed; attempting rollback.")
                db.rollback()
                return False
    except Exception as e:
        logger.error(f"Error during orchestrated run: {e}")
        try:
            db.rollback()
        except Exception:
            pass
        return False
    finally:
        db.disconnect()


def _normalize_steps(step_args: Optional[List[str]]) -> Optional[List[str]]:
    """Normalize step arguments supporting repeated and comma-separated values."""
    if step_args is None:
        return None
    out: List[str] = []
    for s in step_args:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        out.extend(parts)
    return out if out else None


def main_cli(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="SVWS Schuljahr Orchestrator CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    # run command
    p_run = sub.add_parser("run", help="Run selected steps (or all) with optional dry-run")
    p_run.add_argument("--config", default="config.json", help="Path to config.json")
    p_run.add_argument("--dry-run", action="store_true", help="Perform dry-run and rollback changes")
    p_run.add_argument("-s", "--steps", action="append", help="Step keys to run (repeat or comma-separated)")

    # list-steps command
    p_list = sub.add_parser("list-steps", help="List available step keys in order")

    args = parser.parse_args(argv)

    if args.command == "list-steps":
        steps = [k for k, _ in get_available_steps()]
        print("Available steps (in order):")
        for k in steps:
            print(f"- {k}")
        return 0

    if args.command == "run":
        steps = _normalize_steps(args.steps)
        ok = run_all_increments(config_path=args.config, dry_run=bool(args.dry_run), steps=steps)
        return 0 if ok else 1

    return 0

if __name__ == "__main__":
    sys.exit(main_cli())