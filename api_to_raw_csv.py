import csv
import os
import tempfile
import requests
from dotenv import load_dotenv

load_dotenv()

API = "https://api.data.gov/ed/collegescorecard/v1/schools"
API_KEY = os.getenv("SCORECARD_API_KEY")

# DOE Unit Identification Numbers
UNITIDS = [164748, 192110, 167057, 192712, 211893]
YEARS = range(2012, 2022 + 1)

# Request fields only. Names and rates are cleaned later in Snowflake SQL.
FIELD_SUFFIXES = [
    "student.size",
    "admissions.admission_rate.overall",
    "student.retention_rate.four_year.full_time",
    "completion.completion_rate_4yr_150nt",
    "cost.tuition.in_state",
    "cost.avg_net_price.private",
]
NULL_MARKER = r"\N"


def build_csv_filename(years) -> str:
    return f"raw_scorecard_{min(years)}_{max(years)}.csv"


def fetch_year_raw(institution_ids, year):
    if not API_KEY or not API_KEY.strip():
        print("Error: Set SCORECARD_API_KEY in .env before fetching raw data.")
        return None

    institution_ids = list(institution_ids)
    fields = ["id", "school.name"]
    fields += [f"{year}.{suffix}" for suffix in FIELD_SUFFIXES]
    params = {
        "api_key": API_KEY,
        "id__in": ",".join(map(str, institution_ids)),
        "fields": ",".join(fields),
        "per_page": 100,
    }

    # res = response
    # ex = exception
    try:
        res = requests.get(API, params=params, timeout=30)
        res.raise_for_status()
        payload = res.json()
    except requests.exceptions.RequestException as ex:
        print(f"Error fetching data for year {year}: {ex.__class__.__name__}")
        return None
    except ValueError as ex:
        print(f"Error parsing JSON response for year {year}: {ex.__class__.__name__}")
        return None

    expected_ids = set(institution_ids)
    results = payload.get("results") if isinstance(payload, dict) else None
    requested_fields = fields[1:]
    seen_ids = set()
    if isinstance(results, list) and len(results) == len(expected_ids):
        for school in results:
            if not isinstance(school, dict):
                break
            unitid = school.get("id")
            if (
                type(unitid) is not int
                or unitid not in expected_ids
                or unitid in seen_ids
                or any(field not in school for field in requested_fields)
            ):
                break
            seen_ids.add(unitid)
        else:
            if seen_ids == expected_ids:
                return results

    print(f"Error: Incomplete or invalid school data for year {year}.")
    return None


def save_raw_csv(rows, years, output_dir="."):
    filename = build_csv_filename(years)
    path = os.path.join(output_dir, filename)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", dir=output_dir,
            prefix=f".{filename}.", suffix=".tmp", delete=False,
        ) as handle:
            temp_path = handle.name
            writer = csv.DictWriter(handle, fieldnames=["id", "school.name", "year", *FIELD_SUFFIXES])
            writer.writeheader()
            for row in rows:
                if any(value == NULL_MARKER for value in row.values()):
                    raise ValueError("Raw value conflicts with the reserved CSV null marker.")
                writer.writerow({
                    field: NULL_MARKER if value is None else value
                    for field, value in row.items()
                })
        os.replace(temp_path, path)
    finally:
        if temp_path is not None and os.path.exists(temp_path):
            os.unlink(temp_path)
    return filename


def main(output_dir="."):
    if not API_KEY or not API_KEY.strip():
        raise SystemExit("Error: Set SCORECARD_API_KEY in .env before fetching raw data.")

    years = list(YEARS)
    failed_years = []
    rows = []

    for year in years:
        schools = fetch_year_raw(UNITIDS, year)
        if schools is None:
            failed_years.append(year)
            continue

        for school in schools:
            row = {"id": school["id"], "school.name": school["school.name"], "year": year}
            row.update({suffix: school[f"{year}.{suffix}"] for suffix in FIELD_SUFFIXES})
            rows.append(row)

    if failed_years:
        raise SystemExit(
            f"Error: Raw export incomplete for years: {failed_years}. "
            "No files written; previous exports are unchanged."
        )

    if not rows:
        raise SystemExit("Error: No years requested; no raw files written.")

    filename = save_raw_csv(rows, years, output_dir)
    print(f"Saved raw CSV: {filename} ({len(rows)} rows)")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        raise SystemExit(1)
    except SystemExit:
        raise
    except Exception:
        print("Error: Raw CSV export failed. Check the API key, connection, and output directory.")
        raise SystemExit(1)
