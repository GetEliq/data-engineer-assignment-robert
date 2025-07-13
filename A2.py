from typing import List
from datetime import datetime, timedelta, date
from pathlib import Path
import pandas as pd
from pydantic import BaseModel, ValidationError, model_validator
import pytz
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EnergyRecord(BaseModel):
    client_id: str
    date: date
    ext_dev_ref: str
    energy_consumption: List[int]
    resolution: str

    @model_validator(mode="after")
    def validate_resolution_and_length(self):
        expected_lengths = {
            "15min": 96,
            "30min": 48,
            "hourly": 24,
            "daily": 1
        }

        expected = expected_lengths.get(self.resolution.lower())
        actual = len(self.energy_consumption)

        if expected is not None and actual != expected:
            raise ValueError(
                f"Expected {expected} values for resolution '{self.resolution}', got {actual}"
            )

        return self


class EnergyETL:
    def __init__(self, input_path: str, output_path: str, timezone: str = "Europe/Vilnius"):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.timezone = pytz.timezone(timezone)

    def get_interval_minutes(self, resolution: str) -> int:
        mapping = {
            "15min": 15,
            "30min": 30,
            "hourly": 60,
            "daily": 1440
        }
        return mapping.get(resolution.lower(), 60)

    def load_data(self) -> List[EnergyRecord]:
        """
        Read input file in parquet format and validate

        input: Nothing
        return: List of energy record from file
        """
        try:
            raw_df = pd.read_parquet(self.input_path)
        except Exception as e:
            logger.error(f"Failed to read parquet file: {e}")
            return []

        validated_records = []
        for _, row in raw_df.iterrows():
            try:
                record = EnergyRecord(**row.to_dict())
                validated_records.append(record)
            except ValidationError as e:
                logger.warning(f"Validation failed for row:\n{row}\n{e}\n")

        return validated_records

    def transform(self, records: List[EnergyRecord]) -> pd.DataFrame:
        """
        Explode hourly readings into flat rows

        input: List of energy records
        return: energy consumption records
        """
        rows = []
        for record in records:
            base_dt = datetime.combine(record.date, datetime.min.time())
            localized_dt = self.timezone.localize(base_dt)
            interval = self.get_interval_minutes(record.resolution)

            for i, value in enumerate(record.energy_consumption):
                timestamp = localized_dt + timedelta(minutes=i * interval)
                rows.append({
                    "client_id": record.client_id,
                    "ext_dev_ref": record.ext_dev_ref,
                    "timestamp": timestamp,
                    "energy_kWh": value,
                    "resolution": record.resolution
                })

        return pd.DataFrame(rows)

    def main(self):
        """Run the ETL pipeline."""
        records = self.load_data()

        if not records:
            logger.error("No valid records found. Exiting.")
            return

        df_transformed = self.transform(records)
        df_transformed.to_parquet(self.output_path, index=False)
        for client in df_transformed["client_id"].unique():
            subset = df_transformed[df_transformed["client_id"] == client].head(3)
            logger.info(f"\nSample for {client}:\n{subset.to_string(index=False)}")


if __name__ == "__main__":
    etl = EnergyETL(
        input_path="input_data.parquet",
        output_path="flattened_energy_data.parquet"
    )
    etl.main()
