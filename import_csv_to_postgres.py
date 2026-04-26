from __future__ import annotations
import argparse
import csv
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, time
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


BASE_DIR = Path(__file__).resolve().parent
CSV_FILES = {
    "region": BASE_DIR / "region.csv",
    "airline": BASE_DIR / "airline.csv",
    "airport": BASE_DIR / "airport.csv",
    "passenger": BASE_DIR / "passenger.csv",
    "tickets": BASE_DIR / "tickets.csv",
}

REGION_ALIASES = {
    "Hong Kong SAR of China": "Hong Kong",
    "Republic of Korea": "South Korea",
    "DRAGON": "Hong Kong",
}


@dataclass(frozen=True)
class SchemaLayout:
    passenger_name_column: str | None
    passenger_first_name_column: str | None
    passenger_last_name_column: str | None
    flight_instance_date_column: str


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def clean_text(value: str | None) -> str:
    return "" if value is None else value.strip()


def normalize_null(value: str | None) -> str | None:
    text = clean_text(value)
    if not text or text.lower() == "null":
        return None
    return text


def normalize_region_name(value: str | None) -> str:
    text = clean_text(value)
    return REGION_ALIASES.get(text, text)


def normalize_code(value: str | None) -> str | None:
    text = normalize_null(value)
    if text is None:
        return None
    return re.sub(r"\s+", "", text).upper()


def parse_int(value: str | None) -> int:
    return int(clean_text(value))


def parse_decimal(value: str | None) -> Decimal:
    return Decimal(clean_text(value))


def parse_time_field(value: str) -> tuple[time, int]:
    text = clean_text(value)
    offset = 1 if text.endswith("(+1)") else 0
    text = text.replace("(+1)", "")
    hour_str, minute_str = text.split(":", 1)
    return time(int(hour_str), int(minute_str)), offset


def parse_date_field(value: str) -> date:
    year_str, month_str, day_str = clean_text(value).split("/")
    return date(int(year_str), int(month_str), int(day_str))


def detect_schema_layout(cursor, schema_name: str) -> SchemaLayout:
    def columns(table_name: str) -> set[str]:
        cursor.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = %s and table_name = %s
            """,
            (schema_name, table_name),
        )
        return {row[0] for row in cursor.fetchall()}

    passenger_columns = columns("passenger")
    flight_instance_columns = columns("flight_instance")

    if "name" in passenger_columns:
        passenger_name_column = "name"
        passenger_first_name_column = None
        passenger_last_name_column = None
    else:
        passenger_name_column = None
        passenger_first_name_column = "first_name"
        passenger_last_name_column = "last_name"

    if "flight_date" in flight_instance_columns:
        flight_instance_date_column = "flight_date"
    elif "flight_data" in flight_instance_columns:
        flight_instance_date_column = "flight_data"
    else:
        raise RuntimeError("Cannot find flight date column in flight_instance table")

    return SchemaLayout(
        passenger_name_column=passenger_name_column,
        passenger_first_name_column=passenger_first_name_column,
        passenger_last_name_column=passenger_last_name_column,
        flight_instance_date_column=flight_instance_date_column,
    )


def resolve_schema_name(cursor, preferred_schema: str) -> str:
    cursor.execute(
        """
        select table_schema
        from information_schema.tables
        where table_name in ('passenger', 'flight_instance')
        group by table_schema
        having count(distinct table_name) = 2
        order by case when table_schema = %s then 0 else 1 end, table_schema
        limit 1
        """,
        (preferred_schema,),
    )
    row = cursor.fetchone()
    if row is not None:
        return row[0]

    cursor.execute(
        """
        select table_schema
        from information_schema.tables
        where table_name = 'flight_instance'
        order by case when table_schema = %s then 0 else 1 end, table_schema
        limit 1
        """,
        (preferred_schema,),
    )
    row = cursor.fetchone()
    if row is not None:
        return row[0]

    return preferred_schema
def prepare_region_rows(rows: list[dict[str, str]]) -> list[tuple[str, str | None]]:
    seen: set[str] = set()
    prepared: list[tuple[str, str | None]] = []
    for row in rows:
        name = clean_text(row["name"])
        if not name or name in seen:
            continue
        seen.add(name)
        prepared.append((name, normalize_code(row.get("code"))))
    return prepared


def collect_extra_region_names(
    airline_rows: list[dict[str, str]],
    airport_rows: list[dict[str, str]],
    ticket_rows: list[dict[str, str]],
) -> set[str]:
    extra_names: set[str] = set()

    for row in airline_rows:
        region_name = normalize_region_name(row.get("region"))
        if region_name:
            extra_names.add(region_name)

    for row in airport_rows:
        region_name = normalize_region_name(row.get("region"))
        if region_name:
            extra_names.add(region_name)

    for row in ticket_rows:
        source_region = normalize_region_name(row.get("source_region"))
        destination_region = normalize_region_name(row.get("destination_region"))
        airline_region = normalize_region_name(row.get("airline_region"))
        if source_region:
            extra_names.add(source_region)
        if destination_region:
            extra_names.add(destination_region)
        if airline_region:
            extra_names.add(airline_region)

    return extra_names


def infer_airport_name(city: str, code: str) -> str:
    if city:
        return f"{city} Airport"
    return f"Airport {code}"


def synthesize_iata_code(name: str, city: str, used_codes: set[str]) -> str:
    sources = [city, name, f"{city} {name}"]
    for source in sources:
        letters = re.sub(r"[^A-Z]", "", source.upper())
        for start in range(0, max(0, len(letters) - 2)):
            candidate = letters[start : start + 3]
            if len(candidate) == 3 and candidate not in used_codes:
                return candidate

    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for first in alphabet:
        for second in alphabet:
            for third in alphabet:
                candidate = f"{first}{second}{third}"
                if candidate not in used_codes:
                    return candidate

    raise RuntimeError("Unable to synthesize a unique airport code")


def split_full_name(full_name: str) -> tuple[str, str]:
    parts = clean_text(full_name).split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], parts[0]
    return " ".join(parts[:-1]), parts[-1]


def load_database(
    url: str,
    schema_name: str,
    reset: bool,
    tickets_file: Path | None = None,
    page_size: int = 1000,
    timing: bool = False,
) -> None:
    if page_size < 1:
        raise ValueError("page_size must be >= 1")

    active_csv_files = dict(CSV_FILES)
    if tickets_file is not None:
        active_csv_files["tickets"] = tickets_file

    missing_files = [name for name, path in active_csv_files.items() if not path.exists()]
    if missing_files:
        raise FileNotFoundError(f"Missing required CSV files: {', '.join(missing_files)}")

    stage_times: dict[str, float] = {}
    t_all_start = perf_counter()

    t_read_start = perf_counter()
    data = {name: read_csv(path) for name, path in active_csv_files.items()}
    stage_times["read_csv"] = perf_counter() - t_read_start

    airline_rows = data["airline"]
    airport_rows = data["airport"]
    passenger_rows = data["passenger"]
    ticket_rows = data["tickets"]
    extra_region_names = collect_extra_region_names(airline_rows, airport_rows, ticket_rows)
    region_rows = prepare_region_rows(data["region"])

    existing_region_names = {name for name, _code in region_rows}
    for region_name in sorted(extra_region_names - existing_region_names):
        region_rows.append((region_name, None))

    skip_counts: dict[str, int] = defaultdict(int)

    with psycopg2.connect(url) as connection:
        connection.autocommit = False
        with connection.cursor() as cursor:
            resolved_schema_name = resolve_schema_name(cursor, schema_name)
            cursor.execute(sql.SQL("set search_path to {}, public").format(sql.Identifier(resolved_schema_name)))

            layout = detect_schema_layout(cursor, resolved_schema_name)

            if reset:
                cursor.execute(
                    """
                    truncate table
                        flight_instance,
                        flight_route,
                        airport_info,
                        airport,
                        airline,
                        passenger,
                        region
                    restart identity cascade
                    """
                )

            region_insert_rows = [(name, code) for name, code in region_rows]
            region_sql = """
                insert into region (name, code)
                values %s
                on conflict (name) do update
                    set code = excluded.code
                returning region_id, name
            """
            t_stage = perf_counter()
            region_result = execute_values(cursor, region_sql, region_insert_rows, fetch=True, page_size=page_size)
            stage_times["insert_region"] = perf_counter() - t_stage
            region_id_by_name = {name: region_id for region_id, name in region_result}

            airline_insert_rows = []
            for row in airline_rows:
                name = clean_text(row["name"])
                code = normalize_code(row["code"])
                region_name = normalize_region_name(row["region"])
                region_id = region_id_by_name.get(region_name)
                airline_insert_rows.append((code, name, region_id))

            airline_sql = """
                insert into airline (code, name, region_id)
                values %s
                on conflict (name) do update
                    set code = excluded.code,
                        region_id = excluded.region_id
                returning airline_id, name, code
            """
            t_stage = perf_counter()
            airline_result = execute_values(cursor, airline_sql, airline_insert_rows, fetch=True, page_size=page_size)
            stage_times["insert_airline"] = perf_counter() - t_stage
            airline_id_by_name = {name: airline_id for airline_id, name, _code in airline_result}
            airline_id_by_code = {code: airline_id for airline_id, _name, code in airline_result}

            airport_insert_rows: list[tuple[Any, ...]] = []
            airport_info_rows: list[tuple[Any, ...]] = []
            airport_id_by_code: dict[str, int] = {}
            synthetic_airports: list[str] = []
            used_airport_codes: set[str] = set()

            for row in airport_rows:
                code = normalize_code(row["iata_code"])
                if code is None:
                    code = synthesize_iata_code(clean_text(row["name"]), clean_text(row["city"]), used_airport_codes)
                    synthetic_airports.append(code)
                region_name = normalize_region_name(row["region"])
                region_id = region_id_by_name.get(region_name)
                airport_insert_rows.append(
                    (
                        clean_text(row["name"]),
                        clean_text(row["city"]),
                        region_id,
                        code,
                    )
                )
                used_airport_codes.add(code)
                airport_info_rows.append(
                    (
                        code,
                        row["latitude"],
                        row["longitude"],
                        row["altitude"],
                        row["timezone_offset"],
                        row["timezone_dst"],
                        row["timezone_region"],
                    )
                )

            ticket_airport_codes: set[str] = set()
            for row in ticket_rows:
                source_code = normalize_code(row["source_code"])
                if source_code is not None:
                    ticket_airport_codes.add(source_code)

                destination_code = normalize_code(row["destination_code"])
                if destination_code is not None:
                    ticket_airport_codes.add(destination_code)

            existing_codes = {row[3] for row in airport_insert_rows}
            missing_codes = sorted(ticket_airport_codes - existing_codes)

            airport_lookup_rows = defaultdict(list)
            for row in ticket_rows:
                source_code = normalize_code(row["source_code"])
                destination_code = normalize_code(row["destination_code"])
                if source_code:
                    airport_lookup_rows[source_code].append((row["source_city"], row["source_region"]))
                if destination_code:
                    airport_lookup_rows[destination_code].append((row["destination_city"], row["destination_region"]))

            for code in missing_codes:
                city, region_name = airport_lookup_rows[code][0]
                region_name = normalize_region_name(region_name)
                region_id = region_id_by_name.get(region_name)
                airport_insert_rows.append(
                    (
                        infer_airport_name(clean_text(city), code),
                        clean_text(city),
                        region_id,
                        code,
                    )
                )
                airport_info_rows.append((code, None, None, None, None, None, None))
                synthetic_airports.append(code)

            airport_sql = """
                insert into airport (name, city, region_id, iata_code)
                values %s
                on conflict (iata_code) do update
                    set name = excluded.name,
                        city = excluded.city,
                        region_id = excluded.region_id
                returning airport_id, iata_code
            """
            t_stage = perf_counter()
            airport_result = execute_values(cursor, airport_sql, airport_insert_rows, fetch=True, page_size=page_size)
            stage_times["insert_airport"] = perf_counter() - t_stage
            airport_id_by_code.update({code: airport_id for airport_id, code in airport_result})

            airport_info_sql = """
                insert into airport_info
                    (airport_id, latitude, longitude, altitude, timezone_offset, timezone_dst, timezone_region)
                values %s
                on conflict (airport_id) do update
                    set latitude = excluded.latitude,
                        longitude = excluded.longitude,
                        altitude = excluded.altitude,
                        timezone_offset = excluded.timezone_offset,
                        timezone_dst = excluded.timezone_dst,
                        timezone_region = excluded.timezone_region
            """
            airport_info_insert_rows = []
            for code, latitude, longitude, altitude, timezone_offset, timezone_dst, timezone_region in airport_info_rows:
                airport_id = airport_id_by_code.get(code)
                if airport_id is None:
                    skip_counts["airport_info_missing_airport_id"] += 1
                    continue
                airport_info_insert_rows.append(
                    (
                        airport_id,
                        None if normalize_null(latitude) is None else Decimal(clean_text(latitude)),
                        None if normalize_null(longitude) is None else Decimal(clean_text(longitude)),
                        None if normalize_null(altitude) is None else int(clean_text(altitude)),
                        None if normalize_null(timezone_offset) is None else int(clean_text(timezone_offset)),
                        normalize_null(timezone_dst),
                        normalize_null(timezone_region),
                    )
                )
            t_stage = perf_counter()
            execute_values(cursor, airport_info_sql, airport_info_insert_rows, page_size=page_size)
            stage_times["insert_airport_info"] = perf_counter() - t_stage

            passenger_insert_rows = []
            for row in passenger_rows:
                passenger_id = parse_int(row["id"])
                full_name = clean_text(row["name"])
                age = parse_int(row["age"])
                gender = normalize_null(row["gender"])
                if gender is not None:
                    gender = gender.lower()
                mobile_number = re.sub(r"\s+", "", clean_text(row["mobile_number"]))

                if layout.passenger_name_column is not None:
                    passenger_insert_rows.append((passenger_id, full_name, age, gender, mobile_number))
                else:
                    first_name, last_name = split_full_name(full_name)
                    passenger_insert_rows.append((passenger_id, first_name, last_name, age, gender, mobile_number))

            if layout.passenger_name_column is not None:
                passenger_sql = """
                    insert into passenger (passenger_id, name, age, gender, mobile_number)
                    values %s
                    on conflict (passenger_id) do update
                        set name = excluded.name,
                            age = excluded.age,
                            gender = excluded.gender,
                            mobile_number = excluded.mobile_number
                """
            else:
                passenger_sql = """
                    insert into passenger (passenger_id, first_name, last_name, age, gender, mobile_number)
                    values %s
                    on conflict (passenger_id) do update
                        set first_name = excluded.first_name,
                            last_name = excluded.last_name,
                            age = excluded.age,
                            gender = excluded.gender,
                            mobile_number = excluded.mobile_number
                """
            t_stage = perf_counter()
            execute_values(cursor, passenger_sql, passenger_insert_rows, page_size=page_size)
            stage_times["insert_passenger"] = perf_counter() - t_stage

            route_rows = []
            route_key_to_source: dict[tuple[Any, ...], dict[str, str]] = {}
            for row in ticket_rows:
                flight_no = clean_text(row["number"]).upper()
                airline_name = clean_text(row["airline_name"])
                airline_id = airline_id_by_name.get(airline_name)
                if airline_id is None:
                    skip_counts["route_missing_airline"] += 1
                    continue

                source_code = normalize_code(row["source_code"])
                destination_code = normalize_code(row["destination_code"])
                source_airport_id = airport_id_by_code.get(source_code or "")
                destination_airport_id = airport_id_by_code.get(destination_code or "")
                if source_airport_id is None or destination_airport_id is None:
                    skip_counts["route_missing_airport"] += 1
                    continue

                departure_time_value, _departure_offset = parse_time_field(row["departure_time"])
                arrival_time_value, arrival_offset = parse_time_field(row["arrival_time"])
                if arrival_offset == 0 and arrival_time_value < departure_time_value:
                    arrival_offset = 1

                natural_key = (
                    flight_no,
                    airline_id,
                    source_airport_id,
                    destination_airport_id,
                    departure_time_value,
                    arrival_time_value,
                    arrival_offset,
                )
                if natural_key not in route_key_to_source:
                    route_key_to_source[natural_key] = row
                    route_rows.append(natural_key)

            route_sql = """
                insert into flight_route
                    (flight_no, airline_id, source_airport_id, destination_airport_id, departure_time, arrival_time, arrival_day_offset)
                values %s
                on conflict (flight_no, airline_id, source_airport_id, destination_airport_id, departure_time, arrival_time, arrival_day_offset)
                do update set flight_no = excluded.flight_no
                returning route_id, flight_no, airline_id, source_airport_id, destination_airport_id, departure_time, arrival_time, arrival_day_offset
            """
            t_stage = perf_counter()
            route_result = execute_values(cursor, route_sql, route_rows, fetch=True, page_size=page_size)
            stage_times["insert_route"] = perf_counter() - t_stage
            route_id_by_key = {
                (
                    flight_no,
                    airline_id,
                    source_airport_id,
                    destination_airport_id,
                    departure_time_value,
                    arrival_time_value,
                    arrival_day_offset,
                ): route_id
                for route_id, flight_no, airline_id, source_airport_id, destination_airport_id, departure_time_value, arrival_time_value, arrival_day_offset in route_result
            }

            instance_rows = []
            for row in ticket_rows:
                flight_no = clean_text(row["number"]).upper()
                airline_id = airline_id_by_name.get(clean_text(row["airline_name"]))
                if airline_id is None:
                    skip_counts["instance_missing_airline"] += 1
                    continue
                source_airport_id = airport_id_by_code.get(normalize_code(row["source_code"]) or "")
                destination_airport_id = airport_id_by_code.get(normalize_code(row["destination_code"]) or "")
                if source_airport_id is None or destination_airport_id is None:
                    skip_counts["instance_missing_airport"] += 1
                    continue

                departure_time_value, _departure_offset = parse_time_field(row["departure_time"])
                arrival_time_value, arrival_offset = parse_time_field(row["arrival_time"])
                if arrival_offset == 0 and arrival_time_value < departure_time_value:
                    arrival_offset = 1

                route_key = (
                    flight_no,
                    airline_id,
                    source_airport_id,
                    destination_airport_id,
                    departure_time_value,
                    arrival_time_value,
                    arrival_offset,
                )
                route_id = route_id_by_key.get(route_key)
                if route_id is None:
                    skip_counts["instance_missing_route"] += 1
                    continue

                instance_rows.append(
                    (
                        route_id,
                        parse_date_field(row["date"]),
                        parse_decimal(row["business_price"]),
                        parse_int(row["business_remain"]),
                        parse_decimal(row["economy_price"]),
                        parse_int(row["economy_remain"]),
                    )
                )

            instance_sql = f"""
                insert into flight_instance
                    (route_id, {layout.flight_instance_date_column}, business_price, business_remain, economy_price, economy_remain)
                values %s
                on conflict (route_id, {layout.flight_instance_date_column}) do update
                    set business_price = excluded.business_price,
                        business_remain = excluded.business_remain,
                        economy_price = excluded.economy_price,
                        economy_remain = excluded.economy_remain
            """
            t_stage = perf_counter()
            execute_values(cursor, instance_sql, instance_rows, page_size=page_size)
            stage_times["insert_instance"] = perf_counter() - t_stage

        connection.commit()

    stage_times["total"] = perf_counter() - t_all_start

    print("Import finished")
    print(f"Regions imported: {len(region_rows)}")
    print(f"Airlines imported: {len(airline_rows)}")
    print(f"Airports imported: {len(airport_insert_rows)}")
    print(f"Passengers imported: {len(passenger_rows)}")
    print(f"Routes imported: {len(route_rows)}")
    print(f"Flight instances imported: {len(instance_rows)}")
    if synthetic_airports:
        print("Synthetic airport codes added:", ", ".join(synthetic_airports))
    if skip_counts:
        print("Rows skipped during import:")
        for reason, count in sorted(skip_counts.items()):
            print(f"  - {reason}: {count}")
    if timing:
        print("Stage timings (seconds):")
        for stage_name in sorted(stage_times.keys()):
            print(f"  - {stage_name}: {stage_times[stage_name]:.3f}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import CSV data into PostgreSQL")
    parser.add_argument("--dsn", default=os.getenv("DATABASE_URL"), help="PostgreSQL DSN, for example postgresql://user:pass@localhost:5432/project1")
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE", "project1"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "postgres"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--schema", default=os.getenv("PGSCHEMA", "flightdb"))
    parser.add_argument("--reset", action="store_true", help="Truncate target tables before import")
    parser.add_argument("--tickets-file", default=None, help="Optional custom tickets CSV path for volume tests")
    parser.add_argument("--page-size", type=int, default=2000, help="Batch size for execute_values")
    parser.add_argument("--timing", action="store_true", help="Print per-stage timing for Task 3.3 experiments")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    if args.dsn:
        dsn = args.dsn
    else:
        dsn = (
            f"host={args.host} port={args.port} dbname={args.dbname} "
            f"user={args.user} password={args.password}"
        )
    tickets_file = Path(args.tickets_file).resolve() if args.tickets_file else None
    try:
        load_database(
            dsn,
            args.schema,
            args.reset,
            tickets_file=tickets_file,
            page_size=args.page_size,
            timing=args.timing,
        )
    except Exception as exc:
        print(f"Import failed: {exc}")
        raise


if __name__ == "__main__":
    main()