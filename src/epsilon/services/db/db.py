import logging
import mysql.connector
from mysql.connector import MySQLConnection
from src.epsilon.config.settings import settings

log = logging.getLogger(__name__)

def _validate_db_settings() -> None:

    problems: list[str] = []

    try:
        settings.port = int(settings.port)
    except Exception:
        problems.append(f"Invalid PORT: {settings.port}")

    if not str(settings.host).strip():
        problems.append("HOST is empty")
    if not str(settings.user).strip():
        problems.append("USER is empty")
    if settings.password is None:
        problems.append("PASSWORD is None (empty string is allowed, None is not)")
    if not str(settings.database).strip():
        problems.append("DATABASE is empty")
    if not str(settings.table).strip():
        problems.append("TABLE is empty")

    if problems:
        raise ValueError("DB settings error(s): " + "; ".join(problems))

def connect_server() -> MySQLConnection:

    _validate_db_settings()
    try:
        return mysql.connector.connect(
            host=settings.host,
            port=settings.port,
            user=settings.user,
            password=settings.password,
        )
    except Exception as e:
        log.exception("connect_server() failed with settings: host=%r port=%r user=%r",
                      settings.host, settings.port, settings.user)
        raise

def connect_db() -> MySQLConnection:
    _validate_db_settings()
    try:
        return mysql.connector.connect(
            host=settings.host,
            port=settings.port,
            user=settings.user,
            password=settings.password,
            database=settings.database
        )
    except Exception as e:
        log.exception("connect_db() failed with settings: host=%r port=%r user=%r db=%r",
                      settings.host, settings.port, settings.user, settings.database)
        raise

def create_database() -> None:
    conn = connect_server()
    cur = conn.cursor()
    cur.execute(
        f"CREATE DATABASE IF NOT EXISTS {settings.database} "
        "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )
    conn.commit()
    cur.close()
    conn.close()

def create_tables_if_not_exists() -> None:
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {settings.table} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(10) NOT NULL,
            quarter VARCHAR(10) NOT NULL,
            report_date DATE NOT NULL,
            rev_est DECIMAL(20, 2) NULL,
            rev_est_unit VARCHAR(2) NULL,
            rev_act DECIMAL(20, 2) NULL,
            rev_act_unit VARCHAR(2) NULL,
            rev_pct DECIMAL(8, 2) NULL,
            rev_status VARCHAR(10) NULL,
            eps_est DECIMAL(12, 2) NULL,
            eps_act DECIMAL(12, 2) NULL,
            eps_pct DECIMAL(8, 2) NULL,
            eps_status VARCHAR(10) NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_ticker_date (ticker, report_date),
            KEY idx_ticker_date (ticker, report_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    conn.commit()
    cur.close()
    conn.close()