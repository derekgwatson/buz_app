# tests/test_textops.py
import pytest


from services.lead_times.excel_out import (
    rewrite_detailed_preserving_context,
    rewrite_summary_preserving_context,
)


def test_detailed_rewrite_basic():
    old = "\n - Lead Time:  4 - 5 Weeks\n - Location:  Canberra"
    new = rewrite_detailed_preserving_context(old, "6–8 Weeks")
    assert new == "\n - Lead Time:  6–8 Weeks\n - Location:  Canberra"


def test_detailed_rewrite_no_header_leaves_text():
    old = "\n - Location: Canberra\n - Notes: none"
    new = rewrite_detailed_preserving_context(old, "6–8 Weeks")
    assert new == old  # no 'Lead Time:' -> untouched


def test_summary_rewrite_basic_keeps_suffix_after_comma():
    old = "Ready in 4 - 5 Weeks, manufactured locally, lifetime warranty"
    new = rewrite_summary_preserving_context(old, "6–8 Weeks")
    assert new == "Ready in 6–8 Weeks, manufactured locally, lifetime warranty"


def test_summary_rewrite_keeps_bracket_tail_and_suffix():
    old = "Ready in 2 - 3 Weeks (PC Hardware 4-5 Weeks, Others 6 weeks), manufactured locally"
    new = rewrite_summary_preserving_context(old, "6–8 Weeks")
    assert new == "Ready in 6–8 Weeks (PC Hardware 4-5 Weeks, Others 6 weeks), manufactured locally"


def test_summary_rewrite_with_leading_comma_prefix():
    old = ", ready in 4-5 weeks (7 - 8 Weeks shaped, custom), imported, up to 10 years warranty"
    new = rewrite_summary_preserving_context(old, "6–8 Weeks")
    assert new == ", ready in 6–8 Weeks (7 - 8 Weeks shaped, custom), imported, up to 10 years warranty"


def test_summary_rewrite_stops_at_literal_backslash_n():
    old = r"Ready in 4-5 weeks\nAll good"
    new = rewrite_summary_preserving_context(old, "6–8 Weeks")
    assert new == r"Ready in 6–8 Weeks\nAll good"


def test_detailed_rewrite_stops_at_literal_backslash_n():
    old = r" - Lead Time:  4 - 5 Weeks\n - Location:  Canberra"
    new = rewrite_detailed_preserving_context(old, "6–8 Weeks")
    assert new == r" - Lead Time:  6–8 Weeks\n - Location:  Canberra"
