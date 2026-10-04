"""Four offline tests for the raw CSV exporter."""

import csv
import json
from unittest.mock import patch

import pytest
import requests

import api_to_raw_csv as mod


def make_response(year, status=200):
    values = ["0012.5", 0.1234567, None, 0.81234567, "12000", "NULL"]
    payload = {"results": [
        {
            "id": unitid,
            "school.name": f"  Raw school {unitid}  ",
            **{f"{year}.{suffix}": value for suffix, value in zip(mod.FIELD_SUFFIXES, values)},
        }
        for unitid in mod.UNITIDS
    ]}
    response = requests.Response()
    response.status_code = status
    response.encoding = "utf-8"
    response._content = json.dumps(payload).encode("utf-8")
    response.url = f"{mod.API}?api_key=fake-key"
    return response


class TestFetchYearRaw:

    def test_request_uses_scorecard_fields(self, monkeypatch):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")
        response = make_response(2020)

        with patch("api_to_raw_csv.requests.get", return_value=response) as get:
            assert mod.fetch_year_raw(mod.UNITIDS, 2020) == response.json()["results"]

        assert get.call_args.args[0] == mod.API
        params = get.call_args.kwargs["params"]
        assert params["api_key"] == "fake-key"
        assert params["per_page"] == 100
        assert params["id__in"] == "164748,192110,167057,192712,211893"
        assert get.call_args.kwargs["timeout"] == 30
        assert params["fields"].split(",") == [
            "id", "school.name", *[f"2020.{suffix}" for suffix in mod.FIELD_SUFFIXES]
        ]


class TestMain:

    def test_writes_raw_values_unchanged(self, monkeypatch, tmp_path):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")
        monkeypatch.setattr(mod, "YEARS", [2020])

        with patch("api_to_raw_csv.requests.get", return_value=make_response(2020)):
            mod.main(output_dir=tmp_path)

        path = tmp_path / "raw_scorecard_2020_2020.csv"
        with path.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))

        assert {item.name for item in tmp_path.iterdir()} == {path.name}
        assert len(rows) == 5
        assert rows[0] == {
            "id": "164748",
            "school.name": "  Raw school 164748  ",
            "year": "2020",
            "student.size": "0012.5",
            "admissions.admission_rate.overall": "0.1234567",
            "student.retention_rate.four_year.full_time": r"\N",
            "completion.completion_rate_4yr_150nt": "0.81234567",
            "cost.tuition.in_state": "12000",
            "cost.avg_net_price.private": "NULL",
        }

    def test_partial_fetch_failure_writes_nothing_and_does_not_print_api_key(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")
        monkeypatch.setattr(mod, "YEARS", [2020, 2021])

        def get(url, params, timeout):
            if params["fields"].startswith("id,school.name,2020."):
                raise requests.exceptions.Timeout("timed out")
            return make_response(2021)

        with patch("api_to_raw_csv.requests.get", side_effect=get):
            with pytest.raises(SystemExit) as exception:
                mod.main(output_dir=tmp_path)

        assert exception.value.code not in (None, 0)
        assert list(tmp_path.iterdir()) == []
        assert "fake-key" not in capsys.readouterr().out

    def test_http_error_for_every_year_writes_nothing_and_exits(self, monkeypatch, tmp_path, capsys):
        monkeypatch.setattr(mod, "API_KEY", "fake-key")
        monkeypatch.setattr(mod, "YEARS", [2020])

        with patch("api_to_raw_csv.requests.get", return_value=make_response(2020, status=500)):
            with pytest.raises(SystemExit) as exception:
                mod.main(output_dir=tmp_path)

        assert exception.value.code not in (None, 0)
        assert list(tmp_path.iterdir()) == []
        assert "fake-key" not in capsys.readouterr().out
