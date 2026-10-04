# Snowflake preparation

This is an optional add-on to the project.
Python exports raw College Scorecard values; Snowflake will later clean them independently with SQL.

## Local preparation

- Activate the project's virtual environment with its dependencies installed.
- Set `SCORECARD_API_KEY` in `.env`.

From the repository root, run:

```bash
python api_to_raw_csv.py
```

A successful run writes `raw_scorecard_2012_2022.csv` with 55 data rows:
five schools across eleven years, covering 2012–2022.

Its nine columns are `id`, `school.name`, `year`, and the six API metric fields without year prefixes.
Python preserves API names and metric values without name cleaning, rate conversion, or rounding.
`\N` represents API nulls; empty strings and literal `NULL` values stay distinct.
A literal `\N` source value is rejected to keep the null marker unambiguous.

The script validates all eleven responses before writing the output atomically.
A missing key or fetch/validation failure exits unsuccessfully without replacing an existing export.
Only a successful complete run establishes a fresh batch; old files alone do not.

## Snowflake status

Account setup is pending. Warehouse execution has not been verified.
Once the account exists, the later steps are:

1. Manually upload a fresh raw CSV to Snowflake.
2. Clean the data separately using SQL in Snowflake.
3. Manually spot-check early and late school-year rows, rates, and nulls against a fresh export from the original CSV pipeline.

Capture the reference CSV close to the raw export to reduce differences caused by API updates.
Snowflake SQL, sample queries, and comparison tooling are deferred until that work is requested.
