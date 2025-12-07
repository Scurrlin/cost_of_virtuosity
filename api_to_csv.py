import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API = "https://api.data.gov/ed/collegescorecard/v1/schools"
API_KEY = os.getenv("SCORECARD_API_KEY")
UNITIDS = [164748, 192110, 167057, 192712, 211893]
YEARS = range(2012, 2022 + 1)

NAME_MAP = {
    164748: "Berklee College of Music",
    192110: "The Juilliard School",
    167057: "New England Conservatory",
    192712: "Manhattan School of Music",
    211893: "Curtis Institute of Music",
}

FIELD_MAP = {
    "enrollment_total": "student.size",
    "admission_rate": "admissions.admission_rate.overall",
    "retention_rate_ft": "student.retention_rate.four_year.full_time",
    "grad_rate_150": "completion.completion_rate_4yr_150nt",

    # Published tuition + required fees only (no room/board) 
    "tuition_fees": "cost.tuition.in_state",

    # What families actually pay:
    # (Tuition + Room/Board + Books + Personal) - (Federal + State + Institutional grants)
    "avg_net_price": "cost.avg_net_price.private",
}

def fetch_year(institution_ids, year):
    fields = ["id", "school.name"]
    fields += [f"{year}.{f}" for f in FIELD_MAP.values()]
    params = {
        "api_key": API_KEY,
        "id__in": ",".join(map(str, institution_ids)),
        "fields": ",".join(fields),
        "per_page": 100
    }
    
    try:
        r = requests.get(API, params=params, timeout=30)
        r.raise_for_status()
        js = r.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for year {year}: {e}")
        return pd.DataFrame()
    except ValueError as e:
        print(f"Error parsing JSON response for year {year}: {e}")
        return pd.DataFrame()
    
    results = js.get("results", [])
    if not results:
        print(f"Warning: No data returned for year {year}")
        return pd.DataFrame()
    
    rows = []
    for item in results:
        rid = item.get("id")
        if not rid:
            continue
            
        row = {
            "institution": NAME_MAP.get(rid, item.get("school.name", "Unknown")),
            "unitid": rid,
            "year": year,
        }

        for metric, field_suffix in FIELD_MAP.items():
            row[metric] = item.get(f"{year}.{field_suffix}")
        rows.append(row)
    return pd.DataFrame(rows)

def main():
    frames = []
    failed_years = []
    
    for y in YEARS:
        df = fetch_year(UNITIDS, y)
        if not df.empty:
            frames.append(df)
        else:
            failed_years.append(y)
    
    if not frames:
        raise SystemExit("Error: No data retrieved for any years. Check API key and network connection.")
    
    if failed_years:
        print(f"Warning: Failed to retrieve data for years: {failed_years}")
    
    long_df = pd.concat(frames, ignore_index=True)
    
    # Convert select fields to percentages
    percentage_fields = ["admission_rate", "retention_rate_ft", "grad_rate_150"]
    for c in percentage_fields:
        if c in long_df.columns:
            s = pd.to_numeric(long_df[c], errors="coerce")
            # Only convert if all values are valid and are decimals
            if s.notna().any():
                valid_values = s[s.notna()]
                if len(valid_values) > 0 and valid_values.max() <= 1.0 and valid_values.min() >= 0:
                    # Convert to percentage and round to 2 decimal places
                    long_df[c] = (s * 100).round(2)
                else:
                    # If already in percentage format, just round to 2 decimal places
                    long_df[c] = s.round(2)

    sorted_df = long_df.sort_values(['institution', 'year']).reset_index(drop=True)
    year_range = f"{min(YEARS)}_{max(YEARS)}"
    timestamp = datetime.now().strftime("%Y%m%d")
    filename = f"music_school_data_{year_range}_{timestamp}.csv"
    
    sorted_df.to_csv(filename, index=False)
    print(f"Saved data: {filename} ({len(sorted_df)} rows, {len(sorted_df['institution'].unique())} institutions)")
    return sorted_df

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print("Error: Operation failed. Check your API key and network connection.")
        raise SystemExit(1)