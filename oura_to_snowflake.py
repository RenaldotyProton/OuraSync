#!/usr/bin/env python3
"""
Oura Ring → Snowflake ETL
=========================
Récupère les données de l'API Oura v2 et les charge dans Snowflake.
Chargement incrémental : repart du MAX(DAY) connu dans chaque table.

Usage:
    python oura_to_snowflake.py

Prérequis :
    pip install -r requirements.txt
    Renseigner le fichier .env (voir .env.example)
"""

import json
import logging
import os
from datetime import date, timedelta

import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────

OURA_TOKEN   = os.environ["OURA_TOKEN"]
SF_ACCOUNT   = os.environ["SF_ACCOUNT"]   # ex: xy12345.us-east-1
SF_USER      = os.environ["SF_USER"]
SF_PASSWORD  = os.environ["SF_PASSWORD"]
SF_WAREHOUSE = os.environ["SF_WAREHOUSE"]
SF_DATABASE  = os.environ["SF_DATABASE"]
SF_SCHEMA    = os.environ.get("SF_SCHEMA", "OURA")
SF_ROLE      = os.environ.get("SF_ROLE", "")

OURA_BASE    = "https://api.ouraring.com/v2/usercollection"
INITIAL_DATE = "2020-01-01"   # date de départ si la table est vide

# Colonnes à convertir avec TRY_PARSE_JSON lors du MERGE
VARIANT_COLS = {"RAW_DATA"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _v(d: dict, *keys):
    """Accès sécurisé à un dict imbriqué."""
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


# ── Table CALENDAR ────────────────────────────────────────────────────────────

CALENDAR_DDL = """
    CREATE TABLE IF NOT EXISTS {db}.{schema}.CALENDAR (
        DAY         DATE      PRIMARY KEY,
        WEEKDAY     NUMBER(1),   -- 1=Lundi … 7=Dimanche (ISO, convention française)
        WEEKNUMBER  NUMBER(2),   -- Numéro de semaine ISO (1-53)
        YEAR        NUMBER(4),   -- Année civile
        MONTH       NUMBER(2)    -- Mois (1-12)
    )
"""


def populate_calendar(cur) -> None:
    """Insère les jours manquants dans CALENDAR, de la dernière date connue à aujourd'hui."""
    cur.execute(f"SELECT MAX(DAY) FROM {SF_DATABASE}.{SF_SCHEMA}.CALENDAR")
    row  = cur.fetchone()
    last = row[0] if (row and row[0]) else date(2019, 12, 31)

    today = date.today()
    if last >= today:
        log.info("[CALENDAR] À jour.")
        return

    rows = []
    d = last + timedelta(days=1)
    while d <= today:
        rows.append((
            d.strftime("%Y-%m-%d"),  # DAY
            d.isoweekday(),          # WEEKDAY  1=Lun … 7=Dim
            d.isocalendar()[1],      # WEEKNUMBER ISO
            d.year,                  # YEAR (année civile)
            d.month,                 # MONTH
        ))
        d += timedelta(days=1)

    cur.executemany(
        f"INSERT INTO {SF_DATABASE}.{SF_SCHEMA}.CALENDAR "
        f"(DAY, WEEKDAY, WEEKNUMBER, YEAR, MONTH) VALUES (%s, %s, %s, %s, %s)",
        rows,
    )
    log.info(f"[CALENDAR] {len(rows)} jour(s) ajouté(s) (jusqu'au {today})")


# ── Définitions des tables ────────────────────────────────────────────────────

TABLES = {
    "DAILY_CARDIOVASCULAR_AGE": {
        "endpoint": "daily_cardiovascular_age",
        "merge_key": "DAY",   # cet endpoint ne retourne pas de champ 'id'
        "ddl": """
            CREATE TABLE IF NOT EXISTS {db}.{schema}.DAILY_CARDIOVASCULAR_AGE (
                DAY             DATE          PRIMARY KEY,
                VASCULAR_AGE    NUMBER(5),
                RAW_DATA        VARIANT,
                LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """,
        "mapper": lambda r: {
            "DAY":          r.get("day"),
            "VASCULAR_AGE": r.get("vascular_age"),
            "RAW_DATA":     json.dumps(r),
        },
    },

    "DAILY_ACTIVITY": {
        "endpoint": "daily_activity",
        "merge_key": "ID",
        "ddl": """
            CREATE TABLE IF NOT EXISTS {db}.{schema}.DAILY_ACTIVITY (
                ID                          VARCHAR(64)   PRIMARY KEY,
                DAY                         DATE,
                SCORE                       NUMBER(5),
                ACTIVE_CALORIES             NUMBER(10),
                AVERAGE_MET_MINUTES         NUMBER(12,4),
                EQUIVALENT_WALKING_DISTANCE NUMBER(10),
                HIGH_ACTIVITY_TIME          NUMBER(10),
                INACTIVITY_ALERTS           NUMBER(10),
                LOW_ACTIVITY_TIME           NUMBER(10),
                MEDIUM_ACTIVITY_TIME        NUMBER(10),
                NON_WEAR_TIME               NUMBER(10),
                RESTING_TIME                NUMBER(10),
                SEDENTARY_TIME              NUMBER(10),
                STEPS                       NUMBER(10),
                TARGET_CALORIES             NUMBER(10),
                TARGET_METERS               NUMBER(10),
                TOTAL_CALORIES              NUMBER(10),
                MEET_DAILY_TARGETS          NUMBER(5),
                MOVE_EVERY_HOUR             NUMBER(5),
                RECOVERY_TIME               NUMBER(5),
                STAY_ACTIVE                 NUMBER(5),
                TRAINING_FREQUENCY          NUMBER(5),
                TRAINING_VOLUME             NUMBER(5),
                RAW_DATA                    VARIANT,
                LOADED_AT                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """,
        "mapper": lambda r: {
            "ID":                           r.get("id"),
            "DAY":                          r.get("day"),
            "SCORE":                        r.get("score"),
            "ACTIVE_CALORIES":              r.get("active_calories"),
            "AVERAGE_MET_MINUTES":          r.get("average_met_minutes"),
            "EQUIVALENT_WALKING_DISTANCE":  r.get("equivalent_walking_distance"),
            "HIGH_ACTIVITY_TIME":           r.get("high_activity_time"),
            "INACTIVITY_ALERTS":            r.get("inactivity_alerts"),
            "LOW_ACTIVITY_TIME":            r.get("low_activity_time"),
            "MEDIUM_ACTIVITY_TIME":         r.get("medium_activity_time"),
            "NON_WEAR_TIME":                r.get("non_wear_time"),
            "RESTING_TIME":                 r.get("resting_time"),
            "SEDENTARY_TIME":               r.get("sedentary_time"),
            "STEPS":                        r.get("steps"),
            "TARGET_CALORIES":              r.get("target_calories"),
            "TARGET_METERS":                r.get("target_meters"),
            "TOTAL_CALORIES":               r.get("total_calories"),
            "MEET_DAILY_TARGETS":           _v(r, "contributors", "meet_daily_targets"),
            "MOVE_EVERY_HOUR":              _v(r, "contributors", "move_every_hour"),
            "RECOVERY_TIME":                _v(r, "contributors", "recovery_time"),
            "STAY_ACTIVE":                  _v(r, "contributors", "stay_active"),
            "TRAINING_FREQUENCY":           _v(r, "contributors", "training_frequency"),
            "TRAINING_VOLUME":              _v(r, "contributors", "training_volume"),
            "RAW_DATA":                     json.dumps(r),
        },
    },

    "DAILY_READINESS": {
        "endpoint": "daily_readiness",
        "merge_key": "ID",
        "ddl": """
            CREATE TABLE IF NOT EXISTS {db}.{schema}.DAILY_READINESS (
                ID                          VARCHAR(64) PRIMARY KEY,
                DAY                         DATE,
                SCORE                       NUMBER(5),
                TEMPERATURE_DEVIATION       NUMBER(10,4),
                TEMPERATURE_TREND_DEVIATION NUMBER(10,4),
                ACTIVITY_BALANCE            NUMBER(5),
                BODY_TEMPERATURE            NUMBER(5),
                HRV_BALANCE                 NUMBER(5),
                PREVIOUS_DAY_ACTIVITY       NUMBER(5),
                PREVIOUS_NIGHT              NUMBER(5),
                RECOVERY_INDEX              NUMBER(5),
                RESTING_HEART_RATE          NUMBER(5),
                SLEEP_BALANCE               NUMBER(5),
                RAW_DATA                    VARIANT,
                LOADED_AT                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """,
        "mapper": lambda r: {
            "ID":                           r.get("id"),
            "DAY":                          r.get("day"),
            "SCORE":                        r.get("score"),
            "TEMPERATURE_DEVIATION":        r.get("temperature_deviation"),
            "TEMPERATURE_TREND_DEVIATION":  r.get("temperature_trend_deviation"),
            "ACTIVITY_BALANCE":             _v(r, "contributors", "activity_balance"),
            "BODY_TEMPERATURE":             _v(r, "contributors", "body_temperature"),
            "HRV_BALANCE":                  _v(r, "contributors", "hrv_balance"),
            "PREVIOUS_DAY_ACTIVITY":        _v(r, "contributors", "previous_day_activity"),
            "PREVIOUS_NIGHT":               _v(r, "contributors", "previous_night"),
            "RECOVERY_INDEX":               _v(r, "contributors", "recovery_index"),
            "RESTING_HEART_RATE":           _v(r, "contributors", "resting_heart_rate"),
            "SLEEP_BALANCE":                _v(r, "contributors", "sleep_balance"),
            "RAW_DATA":                     json.dumps(r),
        },
    },

    "DAILY_SLEEP": {
        "endpoint": "daily_sleep",
        "merge_key": "ID",
        "ddl": """
            CREATE TABLE IF NOT EXISTS {db}.{schema}.DAILY_SLEEP (
                ID              VARCHAR(64) PRIMARY KEY,
                DAY             DATE,
                SCORE           NUMBER(5),
                DEEP_SLEEP      NUMBER(5),
                EFFICIENCY      NUMBER(5),
                LATENCY         NUMBER(5),
                REM_SLEEP       NUMBER(5),
                RESTFULNESS     NUMBER(5),
                TIMING          NUMBER(5),
                TOTAL_SLEEP     NUMBER(5),
                RAW_DATA        VARIANT,
                LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """,
        "mapper": lambda r: {
            "ID":           r.get("id"),
            "DAY":          r.get("day"),
            "SCORE":        r.get("score"),
            "DEEP_SLEEP":   _v(r, "contributors", "deep_sleep"),
            "EFFICIENCY":   _v(r, "contributors", "efficiency"),
            "LATENCY":      _v(r, "contributors", "latency"),
            "REM_SLEEP":    _v(r, "contributors", "rem_sleep"),
            "RESTFULNESS":  _v(r, "contributors", "restfulness"),
            "TIMING":       _v(r, "contributors", "timing"),
            "TOTAL_SLEEP":  _v(r, "contributors", "total_sleep"),
            "RAW_DATA":     json.dumps(r),
        },
    },

    "DAILY_STRESS": {
        "endpoint": "daily_stress",
        "merge_key": "ID",
        "ddl": """
            CREATE TABLE IF NOT EXISTS {db}.{schema}.DAILY_STRESS (
                ID              VARCHAR(64) PRIMARY KEY,
                DAY             DATE,
                STRESS_HIGH     NUMBER(10),
                RECOVERY_HIGH   NUMBER(10),
                DAY_SUMMARY     VARCHAR(64),
                RAW_DATA        VARIANT,
                LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """,
        "mapper": lambda r: {
            "ID":            r.get("id"),
            "DAY":           r.get("day"),
            "STRESS_HIGH":   r.get("stress_high"),
            "RECOVERY_HIGH": r.get("recovery_high"),
            "DAY_SUMMARY":   r.get("day_summary"),
            "RAW_DATA":      json.dumps(r),
        },
    },

    "SLEEP_SESSIONS": {
        "endpoint": "sleep",
        "merge_key": "ID",
        "ddl": """
            CREATE TABLE IF NOT EXISTS {db}.{schema}.SLEEP_SESSIONS (
                ID                      VARCHAR(64) PRIMARY KEY,
                DAY                     DATE,
                BEDTIME_START           TIMESTAMP_NTZ,
                BEDTIME_END             TIMESTAMP_NTZ,
                AWAKE_TIME              NUMBER(10),
                DEEP_SLEEP_DURATION     NUMBER(10),
                EFFICIENCY              NUMBER(5),
                LATENCY                 NUMBER(10),
                LIGHT_SLEEP_DURATION    NUMBER(10),
                LOWEST_HEART_RATE       NUMBER(5),
                AVERAGE_HRV             NUMBER(8,2),
                REM_SLEEP_DURATION      NUMBER(10),
                RESTLESS_PERIODS        NUMBER(10),
                TIME_IN_BED             NUMBER(10),
                TOTAL_SLEEP_DURATION    NUMBER(10),
                TYPE                    VARCHAR(32),
                RAW_DATA                VARIANT,
                LOADED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """,
        "mapper": lambda r: {
            "ID":                   r.get("id"),
            "DAY":                  r.get("day"),
            "BEDTIME_START":        r.get("bedtime_start"),
            "BEDTIME_END":          r.get("bedtime_end"),
            "AWAKE_TIME":           r.get("awake_time"),
            "DEEP_SLEEP_DURATION":  r.get("deep_sleep_duration"),
            "EFFICIENCY":           r.get("efficiency"),
            "LATENCY":              r.get("latency"),
            "LIGHT_SLEEP_DURATION": r.get("light_sleep_duration"),
            "LOWEST_HEART_RATE":    r.get("lowest_heart_rate"),
            "AVERAGE_HRV":          r.get("average_hrv"),
            "REM_SLEEP_DURATION":   r.get("rem_sleep_duration"),
            "RESTLESS_PERIODS":     r.get("restless_periods"),
            "TIME_IN_BED":          r.get("time_in_bed"),
            "TOTAL_SLEEP_DURATION": r.get("total_sleep_duration"),
            "TYPE":                 r.get("type"),
            "RAW_DATA":             json.dumps(r),
        },
    },

    "WORKOUTS": {
        "endpoint": "workout",
        "merge_key": "ID",
        "ddl": """
            CREATE TABLE IF NOT EXISTS {db}.{schema}.WORKOUTS (
                ID              VARCHAR(64) PRIMARY KEY,
                DAY             DATE,
                ACTIVITY        VARCHAR(64),
                CALORIES        NUMBER(12,2),
                DISTANCE        NUMBER(12,2),
                START_DATETIME  TIMESTAMP_NTZ,
                END_DATETIME    TIMESTAMP_NTZ,
                INTENSITY       VARCHAR(32),
                LABEL           VARCHAR(256),
                SOURCE          VARCHAR(64),
                RAW_DATA        VARIANT,
                LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """,
        "mapper": lambda r: {
            "ID":             r.get("id"),
            "DAY":            r.get("day"),
            "ACTIVITY":       r.get("activity"),
            "CALORIES":       r.get("calories"),
            "DISTANCE":       r.get("distance"),
            "START_DATETIME": r.get("start_datetime"),
            "END_DATETIME":   r.get("end_datetime"),
            "INTENSITY":      r.get("intensity"),
            "LABEL":          r.get("label"),
            "SOURCE":         r.get("source"),
            "RAW_DATA":       json.dumps(r),
        },
    },

    "DAILY_SPO2": {
        "endpoint": "daily_spo2",
        "merge_key": "ID",
        "ddl": """
            CREATE TABLE IF NOT EXISTS {db}.{schema}.DAILY_SPO2 (
                ID              VARCHAR(64) PRIMARY KEY,
                DAY             DATE,
                SPO2_AVERAGE    NUMBER(8,4),
                RAW_DATA        VARIANT,
                LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """,
        "mapper": lambda r: {
            "ID":           r.get("id"),
            "DAY":          r.get("day"),
            "SPO2_AVERAGE": _v(r, "spo2_percentage", "average"),
            "RAW_DATA":     json.dumps(r),
        },
    },

    "VO2_MAX": {
        "endpoint": "vo2_max",
        "merge_key": "ID",
        "ddl": """
            CREATE TABLE IF NOT EXISTS {db}.{schema}.VO2_MAX (
                ID              VARCHAR(64)   PRIMARY KEY,
                DAY             DATE,
                TIMESTAMP       TIMESTAMP_NTZ,
                VO2_MAX         NUMBER(8,4),
                RAW_DATA        VARIANT,
                LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """,
        "mapper": lambda r: {
            "ID":        r.get("id"),
            "DAY":       r.get("day"),
            "TIMESTAMP": r.get("timestamp"),
            "VO2_MAX":   r.get("vo2_max"),
            "RAW_DATA":  json.dumps(r),
        },
    },
}


# ── Oura API ──────────────────────────────────────────────────────────────────

def oura_fetch(endpoint: str, start_date: str, end_date: str) -> list[dict]:
    """Récupère toutes les pages d'un endpoint Oura."""
    headers = {"Authorization": f"Bearer {OURA_TOKEN}"}
    params  = {"start_date": start_date, "end_date": end_date}
    url     = f"{OURA_BASE}/{endpoint}"
    records: list[dict] = []

    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 404:
            log.warning(f"  Endpoint '{endpoint}' indisponible (404) — ignoré")
            return []
        resp.raise_for_status()
        body = resp.json()
        records.extend(body.get("data", []))
        next_token = body.get("next_token")
        if not next_token:
            break
        params = {"next_token": next_token}   # pagination

    return records


# ── Snowflake ─────────────────────────────────────────────────────────────────

def sf_connect() -> snowflake.connector.SnowflakeConnection:
    kw: dict = dict(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )
    if SF_ROLE:
        kw["role"] = SF_ROLE
    return snowflake.connector.connect(**kw)


def get_start_date(cur, table: str) -> str:
    """Retourne MAX(DAY) - 1 jour, ou INITIAL_DATE si la table est vide."""
    try:
        cur.execute(
            f"SELECT MAX(DAY) FROM {SF_DATABASE}.{SF_SCHEMA}.{table}"
        )
        row = cur.fetchone()
        if row and row[0]:
            d = row[0] - timedelta(days=1)   # recalcule le dernier jour (partiel possible)
            return d.strftime("%Y-%m-%d")
    except Exception:
        pass
    return INITIAL_DATE


def ensure_fk(cur, table: str) -> None:
    """
    Ajoute une contrainte FK sur DAY → CALENDAR(DAY).
    Idempotent : l'erreur est silencieusement ignorée si la contrainte existe déjà.
    Note : Snowflake ne fait pas respecter les FK (déclaratives uniquement).
    """
    try:
        cur.execute(
            f"ALTER TABLE {SF_DATABASE}.{SF_SCHEMA}.{table} "
            f"ADD CONSTRAINT FK_{table}_DAY "
            f"FOREIGN KEY (DAY) REFERENCES {SF_DATABASE}.{SF_SCHEMA}.CALENDAR(DAY)"
        )
    except Exception:
        pass  # contrainte déjà existante → on ignore


def upsert(cur, table: str, records: list[dict], merge_key: str = "ID") -> None:
    """
    Charge les records dans une table temporaire de staging (tout en VARCHAR),
    puis exécute un MERGE dans la table cible avec TRY_PARSE_JSON pour les
    colonnes VARIANT.
    merge_key : colonne utilisée pour la clé de déduplication (ID par défaut, DAY pour
    les endpoints qui ne retournent pas de champ id, ex: daily_cardiovascular_age).
    """
    if not records:
        return

    cols    = list(records[0].keys())   # ordre du mapper
    tmp     = f"STG_{table}"

    # 1. Table de staging temporaire (tout VARCHAR pour simplifier l'insert)
    stg_cols = ", ".join(f"{c} VARCHAR(65535)" for c in cols)
    cur.execute(
        f"CREATE OR REPLACE TEMPORARY TABLE {SF_DATABASE}.{SF_SCHEMA}.{tmp} ({stg_cols})"
    )

    # 2. Insert en masse dans le staging
    ph  = ", ".join(["%s"] * len(cols))
    col_str = ", ".join(cols)
    rows = [
        tuple(str(r[c]) if r[c] is not None else None for c in cols)
        for r in records
    ]
    cur.executemany(
        f"INSERT INTO {SF_DATABASE}.{SF_SCHEMA}.{tmp} ({col_str}) VALUES ({ph})",
        rows,
    )

    # 3. MERGE staging → table cible
    def src_expr(col: str) -> str:
        return f"TRY_PARSE_JSON(s.{col})" if col in VARIANT_COLS else f"s.{col}"

    non_key_cols = [c for c in cols if c != merge_key]
    update_set   = ", ".join(f"t.{c} = {src_expr(c)}" for c in non_key_cols)
    ins_cols     = ", ".join(cols)
    ins_vals     = ", ".join(src_expr(c) for c in cols)

    cur.execute(f"""
        MERGE INTO {SF_DATABASE}.{SF_SCHEMA}.{table} t
        USING {SF_DATABASE}.{SF_SCHEMA}.{tmp} s ON t.{merge_key} = s.{merge_key}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
    """)

    log.info(f"  → {table}: {len(records)} lignes traitées ({cur.rowcount} modifiées par le MERGE)")

    cur.execute(f"DROP TABLE IF EXISTS {SF_DATABASE}.{SF_SCHEMA}.{tmp}")


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main() -> None:
    today = date.today().strftime("%Y-%m-%d")
    log.info(f"Démarrage ETL Oura → Snowflake  (jusqu'au {today})")

    conn = sf_connect()
    cur  = conn.cursor()

    try:
        # Crée le schéma si besoin
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA}")

        # ── Calendrier (toujours en premier) ──────────────────────────────────
        cur.execute(CALENDAR_DDL.format(db=SF_DATABASE, schema=SF_SCHEMA))
        populate_calendar(cur)

        for table_name, cfg in TABLES.items():
            log.info(f"[{table_name}]")

            # Crée la table si elle n'existe pas
            cur.execute(cfg["ddl"].format(db=SF_DATABASE, schema=SF_SCHEMA))

            # Ajoute la FK DAY → CALENDAR(DAY) si pas encore présente
            ensure_fk(cur, table_name)

            # Détermine la date de départ
            start = get_start_date(cur, table_name)
            log.info(f"  Plage : {start} → {today}")

            # Récupère depuis l'API Oura
            raw = oura_fetch(cfg["endpoint"], start, today)
            log.info(f"  {len(raw)} enregistrement(s) reçus de l'API")

            if raw:
                mapped = [cfg["mapper"](r) for r in raw]
                upsert(cur, table_name, mapped, cfg.get("merge_key", "ID"))

        conn.commit()
        log.info("ETL terminé avec succès.")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
