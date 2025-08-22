
import datetime
import pandas as pd
from tqdm.auto import tqdm

from . import utils

tqdm.pandas()

class UniqueReport:
    def __init__(self, config):
        self.config = config
        if config.get('bucket_config'):
            bucket_config = config.get('bucket_config')
            bucket_config.update({
                'anon': False,
                'use_listings_cache': False,
                'default_fill_cache': False,
                'config_kwargs': {
                    'signature_version': 's3v4'
                }
            })
            self.bucket_config = bucket_config
            self.bucket = utils.bucket.CustomS3filesystem(**{k:v for k,v in self.bucket_config.items() if k not in ['bucket']})
            
    def _calculate_rate(self, numerator: int, denominator: int) -> float:
        """Helper function to safely calculate a percentage rate."""
        if denominator == 0:
            return 100.0 if numerator == 0 else 0.0
        return round((numerator / denominator) * 100, 2)

    def unique_report(self, table_name: str, date_str=None) -> pd.DataFrame:
        """Calculates Duplicate Count and Rate."""

        input_table = self.config.get('input_table', None)
        subset = self.config.get('subset') or None
        date_now = pd.Timestamp(date_str, tz='UTC') if date_str else pd.Timestamp.now(tz='UTC')        

        total_rows = len(input_table)
        duplicate_count = input_table.duplicated(subset=subset).sum()
        unique_count = total_rows - duplicate_count
        unique_rate_percent = self._calculate_rate(unique_count, total_rows)
        unique_report = pd.DataFrame([{
                "name": table_name,
                "total_rows": total_rows,
                "unique_count": unique_count,
                "uniqueness": unique_rate_percent,
                "timestamp": date_now
                }])
        print(">> Finished check uniqueness!")
        return unique_report
    
    def save(self, table_name: str, date_str=None):
        if getattr(self, 'bucket_config', None) is None:
            raise Exception("Please config `osd_config` before .save()")
        
        df_unique_report = self.unique_report(table_name=table_name, date_str=date_str)

        base_path = f"{self.bucket_config.get('bucket', '')}/landing"
        date_str = date_str if date_str is not None else datetime.datetime.now().strftime('%Y-%m-%d')

        # Save unique report as Parquet
        unique_path = f"{base_path}/_meta/{table_name}_{date_str}-report.parquet"
        print(f"\t saving report to {unique_path}")
        df_unique_report.to_parquet(
            unique_path,
            filesystem=self.bucket,
            engine='pyarrow'
        )

        print("\t save done!")