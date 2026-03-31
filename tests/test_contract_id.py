from market_lake.ids.contract_id import format_strike, make_contract_id

def test_basic():          assert make_contract_id("AAPL","2026-06-19",150,"c") == "AAPL|2026-06-19|150|C"
def test_put():            assert make_contract_id("spy","2025-12-19",500.5,"P") == "SPY|2025-12-19|500.5|P"
def test_round_strike():   assert make_contract_id("QQQ","2026-01-16",400.0,"C") == "QQQ|2026-01-16|400|C"
def test_format_int():     assert format_strike(100) == "100"
def test_format_float():   assert format_strike(99.5) == "99.5"
def test_normalizes_case():
    cid = make_contract_id("aapl","2026-01-01",200,"c")
    assert cid.startswith("AAPL") and cid.endswith("|C")
