 import pandas as pd
from validate_schema import validate_and_cast_df

def test_validate_basic():
    df = pd.DataFrame({
        "user_id":[1,2],
        "event_type":["click","view"],
        "timestamp":["2022-01-01T00:00:00Z","2022-01-01T01:00:00Z"],
        "value":[0,10.5]
    })
    cleaned, dropped = validate_and_cast_df(df)
    assert dropped == 0
    assert cleaned['user_id'].dtype == 'int64'
    assert 'timestamp' in cleaned.columns
