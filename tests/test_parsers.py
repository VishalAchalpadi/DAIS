import pytest

from dais.parsers import get_parser
from dais.spec.models import FixedWidthColumn, ParserConfig


# ---------------------------------------------------------------------------
# fixed_width
# ---------------------------------------------------------------------------

def _fw_config():
    return ParserConfig(
        type="fixed_width",
        columns=[
            FixedWidthColumn(name="account_id", start=1, end=5),
            FixedWidthColumn(name="security_id", start=6, end=10),
            FixedWidthColumn(name="qty", start=11, end=15),
        ],
    )


def test_fixed_width_parses_rows():
    raw = b"ACC01SEC01  100\nACC02SEC02  200\n"
    df = get_parser("fixed_width").parse(raw, _fw_config())
    assert df.columns == ["account_id", "security_id", "qty"]
    assert df.shape == (2, 3)
    assert df["account_id"][0] == "ACC01"
    assert df["security_id"][0] == "SEC01"
    assert df["qty"][0] == "100"


def test_fixed_width_all_string_dtype():
    import polars as pl

    raw = b"ACC01SEC01  100\n"
    df = get_parser("fixed_width").parse(raw, _fw_config())
    assert all(dtype == pl.Utf8 for dtype in df.dtypes)


def test_fixed_width_skips_blank_lines():
    raw = b"ACC01SEC01  100\n\n   \nACC02SEC02  200\n"
    df = get_parser("fixed_width").parse(raw, _fw_config())
    assert df.shape == (2, 3)


def test_fixed_width_empty_file_returns_empty_df_with_columns():
    df = get_parser("fixed_width").parse(b"", _fw_config())
    assert df.columns == ["account_id", "security_id", "qty"]
    assert df.shape == (0, 3)


def test_fixed_width_requires_columns():
    config = ParserConfig(type="fixed_width")
    with pytest.raises(ValueError, match="columns"):
        get_parser("fixed_width").parse(b"anything", config)


# ---------------------------------------------------------------------------
# csv
# ---------------------------------------------------------------------------

def test_csv_parses_with_header():
    raw = b"account_id,security_id,qty\nACC01,SEC01,100\nACC02,SEC02,200\n"
    df = get_parser("csv").parse(raw, ParserConfig(type="csv"))
    assert df.columns == ["account_id", "security_id", "qty"]
    assert df.shape == (2, 3)
    assert df["qty"][0] == "100"


def test_csv_all_columns_are_strings_no_type_inference():
    raw = b"a,b\n1,2\n3,4\n"
    df = get_parser("csv").parse(raw, ParserConfig(type="csv"))
    import polars as pl
    assert all(dtype == pl.Utf8 for dtype in df.dtypes)


# ---------------------------------------------------------------------------
# delimited
# ---------------------------------------------------------------------------

def test_delimited_pipe_separated():
    raw = b"account_id|security_id|qty\nACC01|SEC01|100\n"
    config = ParserConfig(type="delimited", delimiter="|")
    df = get_parser("delimited").parse(raw, config)
    assert df.columns == ["account_id", "security_id", "qty"]
    assert df["qty"][0] == "100"


def test_delimited_no_header():
    raw = b"ACC01|SEC01|100\n"
    config = ParserConfig(type="delimited", delimiter="|", has_header=False)
    df = get_parser("delimited").parse(raw, config)
    assert df.shape == (1, 3)


# ---------------------------------------------------------------------------
# json
# ---------------------------------------------------------------------------

def test_json_array_of_objects():
    raw = b'[{"account_id": "ACC01", "qty": 100}, {"account_id": "ACC02", "qty": 200}]'
    df = get_parser("json").parse(raw, ParserConfig(type="json"))
    assert df.shape == (2, 2)
    assert df["account_id"][0] == "ACC01"
    assert df["qty"][0] == "100"


def test_json_lines():
    raw = b'{"account_id": "ACC01", "qty": 100}\n{"account_id": "ACC02", "qty": 200}\n'
    df = get_parser("json").parse(raw, ParserConfig(type="json", lines=True))
    assert df.shape == (2, 2)
    assert df["account_id"][1] == "ACC02"


def test_json_record_path():
    raw = b'{"meta": {"count": 2}, "data": {"holdings": [{"account_id": "ACC01"}]}}'
    config = ParserConfig(type="json", record_path="data.holdings")
    df = get_parser("json").parse(raw, config)
    assert df.shape == (1, 1)
    assert df["account_id"][0] == "ACC01"


def test_json_null_values_preserved_as_none():
    raw = b'[{"account_id": "ACC01", "qty": null}]'
    df = get_parser("json").parse(raw, ParserConfig(type="json"))
    assert df["qty"][0] is None


# ---------------------------------------------------------------------------
# xml
# ---------------------------------------------------------------------------

def test_xml_flattens_records():
    raw = b"""<?xml version="1.0"?>
    <Holdings>
        <Holding>
            <account_id>ACC01</account_id>
            <security_id>SEC01</security_id>
            <qty>100</qty>
        </Holding>
        <Holding>
            <account_id>ACC02</account_id>
            <security_id>SEC02</security_id>
            <qty>200</qty>
        </Holding>
    </Holdings>
    """
    config = ParserConfig(type="xml", record_xpath="Holding")
    df = get_parser("xml").parse(raw, config)
    assert df.shape == (2, 3)
    assert df["account_id"][0] == "ACC01"
    assert df["qty"][1] == "200"


def test_xml_missing_xpath_raises():
    config = ParserConfig(type="xml")
    with pytest.raises(ValueError, match="record_xpath"):
        get_parser("xml").parse(b"<a></a>", config)


def test_xml_no_matches_returns_empty_df():
    raw = b"<Holdings></Holdings>"
    config = ParserConfig(type="xml", record_xpath="Holding")
    df = get_parser("xml").parse(raw, config)
    assert df.shape == (0, 0)


# ---------------------------------------------------------------------------
# registry
# ---------------------------------------------------------------------------

def test_unknown_parser_type_raises():
    with pytest.raises(ValueError, match="no parser registered"):
        get_parser("carrier_pigeon")


def test_all_five_formats_registered():
    for name in ["csv", "delimited", "fixed_width", "json", "xml"]:
        assert get_parser(name) is not None
