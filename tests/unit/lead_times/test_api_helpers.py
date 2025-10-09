# tests/unit/lead_times/test_api_helpers.py
from services.lead_times import api

def test_strip_only_banner_variants():
    s1 = "\\n***CHRISTMAS CUTOFF 6/11/25 ***Body"
    s2 = "***CHRISTMAS CUTOFF 6/11/25 ***"
    s3 = "Body ***CHRISTMAS CUTOFF 6/11/25 *** tail"
    s4 = "no banners here"
    assert api._strip_only_banner(s1) == "Body"
    assert api._strip_only_banner(s2) == ""
    assert api._strip_only_banner(s3) == "Body  tail"
    assert api._strip_only_banner(s4) == "no banners here"

def test_apply_banner_detailed_text():
    body = "\\n- Lead Time: 2-3 Weeks\\n- Location: Canberra"
    # adds leading \n + banner
    out = api._apply_banner_detailed_text(body, "6/11/25")
    assert out.startswith("\\n***CHRISTMAS CUTOFF 6/11/25 ***")
    assert out.endswith(body)
    # idempotent (existing banner removed then re-added once)
    out2 = api._apply_banner_detailed_text(out, "6/11/25")
    assert out2 == out
    # empty cutoff → strip banners only
    assert api._apply_banner_detailed_text(out, "") == body

def test_apply_banner_summary_text_spacing():
    base = "Ready in 4-5 Weeks, manufactured locally"
    # Append banner with a single separating space
    out = api._apply_banner_summary_text(base, "6/11/25")
    assert out == base + " ***CHRISTMAS CUTOFF 6/11/25 ***"
    # If base already ends with whitespace, no double-space
    out2 = api._apply_banner_summary_text(base + " ", "6/11/25")
    assert out2 == base + " ***CHRISTMAS CUTOFF 6/11/25 ***"

def test_deep_strip_banners_nested():
    obj = {
        "a": "\\n***CHRISTMAS CUTOFF 6/11/25 ***X",
        "b": ["Y", "***CHRISTMAS CUTOFF 1/1/25 ***Z"],
        "c": ("no", "banner"),
    }
    cleaned = api._deep_strip_banners(obj)
    assert cleaned["a"] == "X"
    assert cleaned["b"][0] == "Y"
    assert cleaned["b"][1] == "Z"
    assert cleaned["c"] == ("no", "banner")
