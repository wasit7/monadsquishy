import os
import json
import datetime
import json
import pandas as pd
from tqdm.auto import tqdm
from typing import Dict, Any, Callable, List

from . import utils
from concurrent.futures import ThreadPoolExecutor, as_completed

tqdm.pandas()

allow_state = {'test', 'landing','staging','integration', 'mart'}

class Monad:
    def __init__(self, value):
        self.value = value
        self.status = 'dirty'  # Starting with 'dirty'
        self.output = []
        self.message = []
        self.quality_status = []
        self.dtype = object

    def __or__(self, func):
        if self.status == 'dirty':  # Only process if 'dirty'
            try:
                x = func(self.value)
                self.value = x
                self.status = 'passed'
                self.output.append(self.value)
                self.message.append(f'{func.__name__}()')
                self.quality_status.append(getattr(func, "decorator_name", "passed"))
                return self
            except Exception as e:
                if getattr(e, "value", None) is not None:
                    self.value = e.value    
                self.output.append(self.value)          
                self.message.append(f'{func.__name__}()')
                self.quality_status.append(str(e))
        return self

    def __repr__(self):
        return f'{self.status}: {self.value}, {self.message}, {self.quality_status}'

    def __str__(self):
        return self.__repr__()

    def apply(self, funcs):
        monad = self
        for func in funcs:
            monad = monad | func
        return monad

class DataQualityFramework:
    """
    Implements the data quality metric calculations as defined in the 
    Initial Data Quality Framework document. This class serves as the core
    calculation engine.
    """

    def _calculate_rate(self, numerator: int, denominator: int) -> float:
        """Helper function to safely calculate a percentage rate."""
        if denominator == 0:
            return 100.0 if numerator == 0 else 0.0
        return round((numerator / denominator) * 100, 2)

    def calculate_landing_zone_metrics(self, df: pd.DataFrame, subset: List[str] = None) -> Dict[str, Any]:
        """Calculates Duplicate Count and Rate."""
        total_rows = len(df)
        duplicate_count = df.duplicated(subset=subset).sum()
        unique_count = total_rows - duplicate_count
        unique_rate_percent = self._calculate_rate(unique_count, total_rows)
        return {
            "unique_count": unique_count,
            "uniqueness": unique_rate_percent
        }

    def calculate_staging_zone_metrics(self, df: pd.DataFrame, df_log: pd.DataFrame, df_report: pd.DataFrame, expected_schema: Dict[str, str]) -> Dict[str, Any]:
        """Calculates all specified metrics for the Staging Zone."""
        total_rows = len(df)
        total_cols = len(df.columns)
        total_fields = total_rows * total_cols
        df_quality = df_log.groupby('quality_status').agg({'quality_status':'count'}).T
       
        # 1. Completeness 
        def check_completeness(df: pd.DataFrame) -> pd.Series:
            expected_cols = ['passed', 'inconsistent', 'invalid']
            existing_cols = [col for col in expected_cols if col in df.columns]
            non_null_fields = df[existing_cols].sum().sum()
            return non_null_fields
        non_null_fields = check_completeness(df=df_quality)
        completeness_percent = self._calculate_rate(non_null_fields, total_fields)

        # 2. Validation  
        def check_validation(df: pd.DataFrame) -> pd.DataFrame:
            expected_cols = ['passed', 'inconsistent']
            existing_cols = [col for col in expected_cols if col in df.columns]
            validate_score = df[existing_cols].sum().sum()
            return validate_score
        passed_validate = check_validation(df=df_quality)
        validation_precent = self._calculate_rate(passed_validate, total_fields)

        # 3. Consistency 
        def check_consistency(df: pd.DataFrame) -> pd.DataFrame:
            if 'passed' in df.columns:
                return df['passed'].sum()
            else:
                return 0
        passed_consistency = check_consistency(df=df_quality)
        consistency_percent = self._calculate_rate(passed_consistency, total_fields)

        # 4. Schema Compliance 
        def check_column_schema(df: pd.DataFrame) -> pd.Series :
            result = {}
            for col, expected_dtype in expected_schema.items():
                if col not in df.columns:
                    result[col] = False
                    continue

                actual_dtype = str(df[col].dtype)
                # Fast path: pyarrow types
                if expected_dtype == 'string[pyarrow]':
                    result[col] = pd.api.types.is_string_dtype(df[col])
                elif expected_dtype == 'int64[pyarrow]':
                    result[col] = pd.api.types.is_integer_dtype(df[col])
                elif expected_dtype == 'float64[pyarrow]' or expected_dtype == 'double[pyarrow]':
                    result[col] = pd.api.types.is_float_dtype(df[col])
                elif expected_dtype == 'bool[pyarrow]':
                    result[col] = pd.api.types.is_bool_dtype(df[col])
                elif expected_dtype == 'timestamp[ns, tz=UTC][pyarrow]':
                    # Check the entire column at once (avoid per-cell checks)
                    compliance_time = pd.Series(True, index=df[col].index)
                    for x in df[col]:
                        compliance_time &= pd.api.types.is_datetime64_any_dtype(pd.Series([x])) and hasattr(x, 'tz') and str(x.tz) == 'UTC' and not pd.isna(x)
                    result[col] = compliance_time.all()
                else:
                    result[col] = actual_dtype == expected_dtype 
            
            return pd.Series(result)
        compliant_records = check_column_schema(df=df).sum()
        schema_compliance_percent = self._calculate_rate(compliant_records, len(expected_schema))

        return {
            "completeness": completeness_percent,
            "validation": validation_precent,
            "consistency": consistency_percent,
            "schema": schema_compliance_percent,
            "report": json.dumps(df_report.to_dict(orient='records')),
        }

    def calculate_integration_zone_metrics(self, df_joined: pd.DataFrame, right_table_key_col: str) -> Dict[str, Any]:
        """
        Calculates metrics for the Integration Zone.

        - Uniqueness Rate: Percentage of unique rows in the final joined data.
        - Join Accuracy: Percentage of records from the primary (left) table that successfully joined with the secondary table. 
        """
        # 1. Uniqueness Rate (replaces Deduplication Rate)
        # This measures the uniqueness of the final output data.
        total_rows = len(df_joined)
        duplicate_rows = df_joined.duplicated().sum()
        unique_rows = total_rows - duplicate_rows
        uniqueness_rate_percent = self._calculate_rate(unique_rows, total_rows)

        # 2. Join Accuracy (logic remains the same)
        # This calculation is based on the definition of Join Accuracy. 
        records_attempted_join = len(df_joined)
        correctly_joined_records = df_joined[right_table_key_col].notna().sum()
        join_accuracy_percent = self._calculate_rate(correctly_joined_records, records_attempted_join)

        return {
            "uniqueness": uniqueness_rate_percent,
            "join_accuracy": join_accuracy_percent,
        }

    def calculate_datamart_zone_metrics(self, df: pd.DataFrame, anomaly_rules: Dict[str, Callable]) -> Dict[str, Any]:
        """Calculates metrics for the Data Mart Zone."""
        total_rows = len(df)
        
        # 1. Anomaly Rate 
        def is_anomalous(row):
            for rule_func in anomaly_rules.values():
                if rule_func(row): return True
            return False
        anomalous_records = df.apply(is_anomalous, axis=1).sum()
        anomaly_rate_percent = self._calculate_rate(anomalous_records, total_rows)
        
        return {"anomaly_rate_percent": anomaly_rate_percent}

class Squishy:
    def __init__(self, config):
        self.config = config
        if (config.get('state', None) in allow_state) == False:  # noqa: E712
            raise Exception(f"state must be {allow_state}")
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
        # Squishy now has an instance of the framework to delegate calculations to.
        self.dq_framework = DataQualityFramework()
        
    def apply_transformations(self, df, column_name, monad_funcs):
        """
        Applies Monad transformations to a dataframe column using pandas apply().
        
        Parameters:
        - df: pd.DataFrame, the input dataframe.
        - column_name: str, the column to apply transformations to.
        - monad_funcs: list of functions to apply in the Monad pipeline.
    
        Returns:
        - pd.DataFrame: a dataframe with transformation results in a new column 'result'.
        """
        df = df[[column_name]].copy()
        df['input'] = df[column_name]
        df['monad_result'] = df[column_name].apply(Monad)
        df['monad_result'] = df['monad_result'].progress_apply(
            lambda x: x.apply(monad_funcs),
        )
        df_transformed=pd.DataFrame({
            'input':df['input'],
            'output':df['monad_result'].apply(lambda x: x.output),
            'value':df['monad_result'].apply(lambda x: x.value),
            'message':df['monad_result'].apply(lambda x: x.message),
            'quality_status':df['monad_result'].apply(lambda x: x.quality_status),
        })
        return df_transformed
    
    def explode(self, df_transformed):
        df_exploded = df_transformed.copy()
        df_exploded['zipped'] = df_exploded.apply(
            lambda row: list(zip(row['output'], row['message'], row['quality_status'])),
            axis=1
        )
        df_exploded = df_exploded.explode('zipped').reset_index(names=['row'])
        df_exploded[['output', 'message', 'quality_status']] = pd.DataFrame(df_exploded['zipped'].tolist(), index=df_exploded.index)
        df_exploded.drop(columns='zipped', inplace=True)
        df_exploded.insert(4, 'is_passed', df_exploded.quality_status.str.match("passed"))
        return df_exploded
    
    def create_dir(self, path):
        os.makedirs(path, exist_ok=True)

    def process_column(self, _df, out_col, in_col, funcs):
        print(f"Output: {out_col}")
        print(f'''Input: {in_col[:20]:20}\nProcess: {str([f.__name__ for f in funcs])}''')
        
        df_transformed = self.apply_transformations(_df, in_col, funcs)
        df_exploded = self.explode(df_transformed)

        df_transformed_result = pd.Series(df_transformed['value'], name=out_col)
        
        df_exploded.insert(1, 'in_column', in_col)
        df_exploded.insert(2, 'out_column', out_col)
        
        return df_transformed_result, df_exploded
            
    def all_transformations(self, sq_config):
        for pull in sq_config.get('transformations', []):
            df_all_transformed = pd.DataFrame()
            df_all_exploded = pd.DataFrame()
            _df = pull['input_table']
            
            futures = []
            with ThreadPoolExecutor() as executor:
                for out_col, v in pull['out_columns'].items():
                    in_col = v['input']
                    funcs = v['funcs']
                    if funcs[-1].decorator_name != 'passed': # Check last function is not consistency auto add end function
                        def end(x):
                            return x
                        funcs.append(end)
                    futures.append(
                        executor.submit(self.process_column, _df, out_col, in_col, funcs)
                    )

                for future in as_completed(futures):
                    try:
                        df_transformed_col, df_exploded_col = future.result()
                        df_all_transformed[df_transformed_col.name] = df_transformed_col
                        df_all_exploded = pd.concat([df_all_exploded, df_exploded_col])
                    except Exception as e:
                        print("Error in transformation:", e)

            # Finalize
            df_all_exploded = df_all_exploded.reset_index(drop=True).rename(columns={
                "row": "input_row", 
                "in_column": "input_column",
                "out_column": "output_column", 
                "input": "input_value", 
                "output": "output_value",
                # "value": "value"
            }).drop(columns=['value'])

            # Save transformed
            path = pull['transformed_path']
            self.create_dir(path)
            df_all_transformed = df_all_transformed[_df.columns]
            df_all_transformed.to_parquet(os.path.join(path, 'transformed.parquet'))

            # Save exploded
            path = pull['exploded_path']
            self.create_dir(path)
            df_all_exploded['input_value'] = df_all_exploded['input_value'].astype(str)
            df_all_exploded['output_value'] = df_all_exploded['output_value'].astype(str)
            df_all_exploded = df_all_exploded.sort_values(by=['input_column', 'output_column']).reset_index(drop=True)
            df_all_exploded.to_parquet(os.path.join(path, 'exploded.parquet'))

        print('>> Finished transformations!')

        for pull in sq_config.get('generation',[]):
            # df_all_transformed=pd.DataFrame()
            df_all_exploded=pd.DataFrame()
            df_all_transformed = pull['input_table']
            path = pull['transformed_path']
            self.create_dir(path)
            df_all_transformed.to_parquet(os.path.join(path,'transformed.parquet'))
            # chain of thought here
        
    def run(self):
        return self.all_transformations(self.config)
        
    def input(self, index=0):
        return self.config['transformations'][index]['input_table']

    def output(self, index=0):
        _ = self.config.get('transformations') or self.config.get('generation')
        path = _[index]['transformed_path']
        if not os.path.exists(path):
            # Create a dummy dataframe if it doesn't exist for demonstration
            print(f"Warning: Output file not found at {path}. Creating a dummy empty DataFrame.")
            return pd.DataFrame()
        
        return pd.read_parquet(os.path.join(path,'transformed.parquet'), dtype_backend='pyarrow')

    def log(self, index=0):
        path = self.config['transformations'][index]['exploded_path']
        return pd.read_parquet(os.path.join(path,'exploded.parquet'))
    
    def get_output_column(self, index=0):
        return self.config['transformations'][index]['out_columns']
    
    def dirty_report(self, index=0):
        df_log = self.log(index)
        # Filter the dataframe for rows where 'is_passed' is False
        df_not_passed = df_log[(df_log['is_passed'] == False) & (df_log['quality_status'] != 'not_missing') & (df_log['quality_status'] != 'valid')]  # noqa: E712

        # Create the pivot table to count occurrences of failed rows
        df_pivot_report = pd.pivot_table(
            df_not_passed,
            values='is_passed',  # The value to aggregate
            index=['input_column', 'output_column', 'input_value', 'quality_status'],  # Grouping columns
            aggfunc='count',  # Aggregate function to count occurrences
            # dropna=False,  # Do not drop missing values
            # fill_value=None  # Use NaN when there are no values
        )

        # Resetting the index to flatten the pivot table
        df_pivot_report = df_pivot_report.reset_index()
        df_pivot_report = df_pivot_report.rename(columns={'is_passed': 'dirty_count', 'quality_status': 'dirty_status'})

        # Renaming the columns for clarity
        df_pivot_report.columns = ['input_column', 'output_column', 'input_value', 'dirty_status', 'dirty_count']
        # df_pivot_report = df_pivot_report.sort_values(['out_column','dirty_count'], ascending=False)
        df = df_pivot_report[df_pivot_report['dirty_count'].notna()].copy()
        df = df[['input_column', 'output_column', 'input_value', 'dirty_count', 'dirty_status']]
        order = self.get_output_column(index)
        df['output_column'] = pd.Categorical(df['output_column'], categories=order, ordered=True)
        df_sorted = df.sort_values(by=['dirty_count'], ascending=False).reset_index(drop=True)
        return df_sorted

    def clean_report(self, index=0):
        df_log = self.log(index)
        ## clean_report
        df_passed = df_log[df_log['is_passed'] == True]  # noqa: E712
        # Create the pivot table to count occurrences of failed rows
        df_pivot_report = pd.pivot_table(
            df_passed,
            values='is_passed',  # The value to aggregate
            index=['input_column', 'output_column', 'output_value'],  # Grouping columns
            aggfunc='count',  # Aggregate function to count occurrences
            dropna=False,  # Do not drop missing values
            # fill_value=None  # Use NaN when there are no values
        )

        # Resetting the index to flatten the pivot table
        df_pivot_report = df_pivot_report.reset_index()

        # Renaming the columns for clarity
        df_pivot_report.columns = ['input_column', 'output_column', 'output_value', 'clean_count']
        df = df_pivot_report.copy()
        order = self.get_output_column(index)
        df['output_column'] = pd.Categorical(df['output_column'], categories=order, ordered=True)
        df_sorted = df.sort_values(by=['output_column', 'output_value'], ascending=False)
        return df_sorted

    def report(self, table_name: str) -> pd.DataFrame:
        """_summary_

        Args:
            table_name (str): table name

        Returns:
            pd.DataFrame: report dataframe
        """
        df = self.output()
        op_len = len(df)
        fields = df.columns.tolist()

        df_log = self.log()

        _df_log = df_log.groupby(['output_column', 'quality_status']).size().unstack(fill_value=0)
        
        # Calculate clean, inconsistent, invalid, and missing data counts
        clean_data = (
            pd.Series(_df_log['passed'], name='count').fillna(0).astype(int)
            if 'passed' in _df_log.columns
            else pd.Series(0, index=_df_log.index, name='count', dtype=int)
        ).reindex(index=fields)

        inconsist_data = (
            pd.Series(_df_log['inconsistent'], name='count').fillna(0).astype(int)
            if 'inconsistent' in _df_log.columns
            else pd.Series(0, index=_df_log.index, name='count', dtype=int)
        ).reindex(index=fields)

        invalid_data = (
            pd.Series(_df_log['invalid'], name='count').fillna(0).astype(int)
            if 'invalid' in _df_log.columns
            else pd.Series(0, index=_df_log.index, name='count', dtype=int)
        ).reindex(index=fields)

        missing_data = (
            pd.Series(_df_log['missing'], name='count').fillna(0).astype(int)
            if 'missing' in _df_log.columns
            else pd.Series(0, index=_df_log.index, name='count', dtype=int)
        ).reindex(index=fields)

        
        nt = missing_data + invalid_data + inconsist_data + clean_data
        ng = nt - missing_data - invalid_data - inconsist_data  # noqa: F841

        complete_percent = ((1 - (missing_data / nt )) * 100).round(2)
        validation_percent = ((1 - (invalid_data / (nt-missing_data))) * 100).round(2)
        consistency_percent = ((1 - (inconsist_data / (nt-missing_data-invalid_data))) * 100).round(2)

        # Create the resulting DataFrame
        report_df = pd.DataFrame({
            # "Table": table_name,
            "Field Name": fields,
            "Total Rows": op_len,
            "Missing": missing_data,
            "Invalid": invalid_data,
            "Inconsistent": inconsist_data,
            "Clean": clean_data,
            "Completeness": complete_percent,
            "Validity": validation_percent,
            "Consistency": consistency_percent,
        }).reset_index(drop=True)
        
        return report_df

    def _generate_metrics_report(self, df: pd.DataFrame, table_name: str, date_str=None) -> pd.DataFrame:
        """
        Generates a summary metrics report by delegating calculations to the
        DataQualityFramework instance. The behavior is controlled by the 'state' 
        and 'metrics_config' sections of the main configuration.
        """
        state = self.config.get("state")
        metrics_config = self.config.get("metrics_config", {})
        now = pd.Timestamp(date_str, tz='UTC') if date_str else pd.Timestamp.now(tz='UTC')

        report_data = {'name': table_name, 'timestamp': now, 'total_rows': len(df)}
        metrics = {}

        print(f"\nGenerating '{state}' zone report using DataQualityFramework logic...")

        if state == 'landing':
            metrics = self.dq_framework.calculate_landing_zone_metrics(df)

        elif state == 'staging':
            # These rules must be defined in the run configuration
            expected_schema = metrics_config.get('expected_schema', {})
            
            # Get validate score from report
            df_log = self.log()
            df_report = self.report(table_name=table_name)
            if not expected_schema:
                raise ValueError("Staging report requires 'expected_schema' in metrics_config")
            metrics = self.dq_framework.calculate_staging_zone_metrics(df, df_log, df_report, expected_schema)

        elif state == 'integration':
            # The Integration Zone focuses on the success of deduplication and join operations.
            # This metric has been updated to check the uniqueness of the final joined data.
            right_table_key = metrics_config.get('right_table_key_col')
            metrics = self.dq_framework.calculate_integration_zone_metrics(df, right_table_key)

        elif state == 'mart':
            # This state requires anomaly detection rules
            anomaly_rules = metrics_config.get('anomaly_rules', {})
            if not anomaly_rules:
                raise ValueError("Mart report requires 'anomaly_rules' in metrics_config")
            metrics = self.dq_framework.calculate_datamart_zone_metrics(df, anomaly_rules)

        else:
            raise Exception(f"Unsupported state for report generation: {state}")
        
        report_data.update(metrics)
        return pd.DataFrame([report_data])
    
    def save(self, table_name, date_str=None):
        if getattr(self, 'bucket_config', None) is None:
            raise Exception("Please config `osd_config` before .save()")

        df_output = self.output()

        base_path = f"{self.bucket_config.get('bucket', '')}/{self.config.get('state')}"
        date_str = date_str if date_str is not None else datetime.datetime.now().strftime('%Y-%m-%d')
        
        # Save output table
        output_path = f"{base_path}/{table_name}/{date_str}.parquet"
        print(f"\t saving transformed data to {output_path}")
        df_output.to_parquet(
            output_path,
            filesystem=self.bucket,
            engine='pyarrow'
        )

        # Generate metrics report depending on state
        df_report = self._generate_metrics_report(df_output, table_name, date_str)

        # Save report as Parquet
        report_path = f"{base_path}/_meta/{table_name}_{date_str}-report.parquet"
        print(f"\t saving report to {report_path}")
        df_report.to_parquet(
            report_path,
            filesystem=self.bucket,
            engine='pyarrow'
        )

        print("\t save done!")