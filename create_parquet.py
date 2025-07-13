import pandas as pd

df = pd.DataFrame([
    {
        "client_id": "client_hourly",
        "date": pd.to_datetime("2025-07-13").date(),
        "ext_dev_ref": "meter_001",
        "energy_consumption": [1] * 24,
        "resolution": "hourly"
    },
    {
        "client_id": "client_15min",
        "date": pd.to_datetime("2025-07-13").date(),
        "ext_dev_ref": "meter_002",
        "energy_consumption": [2] * 96,
        "resolution": "15min"
    },
    {
        "client_id": "client_30min",
        "date": pd.to_datetime("2025-07-13").date(),
        "ext_dev_ref": "meter_003",
        "energy_consumption": [3] * 48,
        "resolution": "30min"
    },
    {
        "client_id": "client_daily",
        "date": pd.to_datetime("2025-07-13").date(),
        "ext_dev_ref": "meter_004",
        "energy_consumption": [4],
        "resolution": "daily"
    }
])

df.to_parquet("input_data.parquet", index=False, engine="pyarrow")
