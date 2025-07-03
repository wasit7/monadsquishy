import os
import json
import datetime
import pandas as pd
import pyarrow as pa
from tqdm.auto import tqdm
from typing import Dict, Any, Callable, List

from . import utils

tqdm.pandas()
class Monad:
    def __init__(self, value):
        self.value = value
        self.status = 'dirty'  # Starting with 'dirty'
        self.message = []
        self.dtype = object

    def __or__(self, func):
        if 'dirty' == self.status != 'missing': # Only process if 'dirty' and not 'missing'
            try:
                x = func(self.value)
                if x is not None and pd.notna(x) and x is not pd.NaT: # Process for Passed data
                    self.value = x
                    self.status = 'passed'
                    self.message.append(f'Passed: {func.__name__}()')
                else: # Process for Missing data
                    self.value = None
                    self.status = 'missing'
                    self.message.append(f'Missing: {func.__name__}()')
                return self
            except Exception as e:
                self.message.append(f'Failed: {func.__name__}(): {str(e)}')
        return self

    def __repr__(self):
        return f'{self.status}({self.value}) {self.message}'

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

    def calculate_staging_zone_metrics(self, df: pd.DataFrame, validate_score: pd.Series, consistency_rules: Dict[str, Callable], expected_schema: Dict[str, str]) -> Dict[str, Any]:
        """Calculates all specified metrics for the Staging Zone."""
        total_rows = len(df)
        total_cols = len(df.columns)
        total_fields = total_rows * total_cols

        # 1. Completeness 
        non_null_fields = df.notna().sum().sum()
        completeness_percent = self._calculate_rate(non_null_fields, total_fields)

        # 2. Consistency 
        def check_row_consistency(row):
            result = {}
            rules = consistency_rules  # ensure it's a dict
            for col in row.index:
                if col in rules:
                    rule_func = rules[col]
                    result[col] = rule_func(row[col])
                # else:
                #     result[col] = True  # If there's no rule define, Set True
            return pd.Series(result)
                
        passed_consistency = df.apply(check_row_consistency, axis=1).sum().sum()
        consistency_percent = self._calculate_rate(passed_consistency, len(consistency_rules)*total_rows)

        # 3. Validation Failures 
        passed_validate = validate_score.sum().sum()
        validation_failures = self._calculate_rate(passed_validate, total_fields)

        # 4. Schema Compliance 
        def check_column_schema(col):
            # Mapping from string types to PyArrow types
            type_map = {
                'string': pa.string(),
                'int': pa.int64(),
                'float': pa.float64(),
                'double': pa.float64(),
                'boolean': pa.bool_(),
                'datetime': pa.timestamp('ns', tz='UTC')
            }

            pyarrow_map = {
                'string[pyarrow]': 'string',
                'int64[pyarrow]': 'int64',
                'float64[pyarrow]': 'float64',
                'double[pyarrow]': 'float64',
                'bool[pyarrow]': 'bool',
                'timestamp[ns, tz=UTC][pyarrow]': 'timestamp[ns, tz=UTC]',
            }

            actual_dtype = pyarrow_map.get(str(col.dtype))
            # print(f"Column: {col.name}, actual_dtype: {str(col.dtype)}")

            expected_dtype = type_map.get(expected_schema.get(col.name))
            result = {}
            if actual_dtype == str(expected_dtype):
                for index, val in enumerate(col):     
                    try:
                        if pa.types.is_string(expected_dtype):
                            if not isinstance(val, str) and pd.isna(val):
                                result[str(index)] = pd.api.types.is_string_dtype(col)
                                continue
                            result[str(index)] = True
                        
                        elif pa.types.is_integer(expected_dtype):
                            if not isinstance(val, int) and pd.isna(val):
                                result[str(index)] = pd.api.types.is_int64_dtype(col)
                                continue
                            result[str(index)] = True

                        elif pa.types.is_floating(expected_dtype):
                            if not isinstance(val, float) and pd.isna(val):
                                result[str(index)] = pd.api.types.is_float_dtype(col)
                                continue
                            result[str(index)] = True

                        elif pa.types.is_boolean(expected_dtype):
                            if not isinstance(val, bool) and pd.isna(val):
                                result[str(index)] = pd.api.types.is_bool_dtype(col)
                                continue
                            result[str(index)] = True
                        
                        elif pa.types.is_timestamp(expected_dtype):
                            if not isinstance(val, pd.Timestamp) and pd.isna(val) and (val is pd.NaT):
                                result[str(index)] = pd.api.types.is_timedelta64_ns_dtype(col)
                                continue
                            result[str(index)] = True
                    except Exception:
                        result[str(index)] = False
                return pd.Series(result)
            else:
                return pd.Series([False] * len(col), index=col.index)
        compliant_records = df.apply(check_column_schema, axis=0).all().sum()
        schema_compliance_percent = self._calculate_rate(compliant_records, total_cols)

        return {
            "completeness": completeness_percent,
            "validation": validation_failures,
            "consistency": consistency_percent,
            "schema": schema_compliance_percent
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

allow_state = {'test', 'landing','staging','integration', 'mart'}


class Squishy:
    def __init__(self, config):
        self.config = config
        if (config.get('state', None) in allow_state) == False:
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
        df['monad_result'] = df[column_name].progress_apply(
            lambda x: Monad(x).apply(monad_funcs),
            
        )
        df_transformed=pd.DataFrame({
            'input':df['input'],
            'value':df['monad_result'].apply(lambda x: x.value),
            'status':df['monad_result'].apply(lambda x: x.status),
            'message':df['monad_result'].apply(lambda x: x.message),
            
        })
        return df_transformed
    
    def explode(self,df_transformed):
        df_exploded=df_transformed.explode('message').reset_index(names=['row'])
        df_exploded.insert(3, 'is_passed', df_exploded.message.str.match("Passed:"))
        # df_exploded['is_passed']=df_exploded.message.str.match("Passed:")
        return df_exploded
    
    def create_dir(self, path):
        os.makedirs(path, exist_ok=True)
            
    def all_transformations(self, sq_config):
        # all_transformed=[]
        # all_exploded=[]
        for pull in sq_config.get('transformations',[]):
            df_all_transformed = pd.DataFrame()
            df_all_exploded = pd.DataFrame()
            _df = pull['input_table']
            for i,x in enumerate(pull['out_columns'].items()):
                k,v=x
                out_col = k
                in_col = v['input']
                funcs = v['funcs']
                print(f"{i + 1}/{len(pull['out_columns'].items())} Output: {out_col}")
                print(f'''Input: {in_col[:20]:20}\nProcess: {str([ f.__name__ for f in funcs])}''')
                df_transformed = self.apply_transformations(_df, in_col, funcs)
                data = df_transformed['value']                
                if v.get('dtype'): 
                    data = data.astype(v.get('dtype'))
                df_all_transformed[out_col]= data # must be string
                df_exploded = self.explode(df_transformed)
                df_exploded.insert(1, 'in_column', in_col)
                df_exploded.insert(2, 'out_column', out_col)
                df_all_exploded = pd.concat([df_all_exploded, df_exploded])
            
            # finalizing the report table by exploding the message    
            df_all_exploded = df_all_exploded.reset_index(drop=True)\
                .rename(columns={
                    "row": "input_row", 
                    "in_column": "input_column",
                    "out_column": "output_column", 
                    "input": "input_value", 
                    "value":"output_value"
                }, errors="raise")
            # print(df_all_exploded)
           
            # save transformed table to file
            path = pull['transformed_path']
            self.create_dir(path)
            df_all_transformed.to_parquet(os.path.join(path,'transformed.parquet'), engine='pyarrow')

            # save transformed table to file
            path = pull['exploded_path']
            self.create_dir(path)
            df_all_exploded['input_value'] = df_all_exploded['input_value'].astype(str)
            df_all_exploded['output_value'] = df_all_exploded['output_value'].astype(str)
            df_all_exploded.to_parquet(os.path.join(path,'exploded.parquet'))

            # append the global list
            # all_transformed.append(df_all_transformed)
            # all_exploded.append(df_all_exploded)
        # return all_transformed, all_exploded
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
        ## dirty report
        df_last = df_log.drop_duplicates(['input_row','output_column','input_value'], keep='last')
        # Filter the dataframe for rows where 'is_passed' is False
        df_not_passed = df_last[(df_last['is_passed'] == False) & (df_last['status'] == 'dirty')]  # noqa: E712

        # Create the pivot table to count occurrences of failed rows
        df_pivot_report = pd.pivot_table(
            df_not_passed,
            values='is_passed',  # The value to aggregate
            index=['input_column', 'output_column', 'input_value'],  # Grouping columns
            aggfunc='count',  # Aggregate function to count occurrences
            dropna=False,  # Do not drop missing values
            # fill_value=None  # Use NaN when there are no values
        )

        # Resetting the index to flatten the pivot table
        df_pivot_report = df_pivot_report.reset_index()

        # Renaming the columns for clarity
        df_pivot_report.columns = ['input_column', 'output_column', 'input_value', 'dirty_count']
        # df_pivot_report = df_pivot_report.sort_values(['out_column','dirty_count'], ascending=False)
        df = df_pivot_report[df_pivot_report['dirty_count'].notna()].copy()
        order = self.get_output_column(index)
        df['output_column'] = pd.Categorical(df['output_column'], categories=order, ordered=True)
        df_sorted = df.sort_values(by=['output_column','dirty_count'], ascending=False).reset_index(drop=True)
        return df_sorted

    def clean_report(self, index=0):
        df_log = self.log(index)
        ## clean_report
        df_not_passed = df_log[df_log['is_passed'] == True]
        # Create the pivot table to count occurrences of failed rows
        df_pivot_report = pd.pivot_table(
            df_not_passed,
            values='is_passed',  # The value to aggregate
            index=['input_column', 'output_column', 'message'],  # Grouping columns
            aggfunc='count',  # Aggregate function to count occurrences
            # dropna=False,  # Do not drop missing values
            fill_value=None  # Use NaN when there are no values
        )

        # Resetting the index to flatten the pivot table
        df_pivot_report = df_pivot_report.reset_index()

        # Renaming the columns for clarity
        df_pivot_report.columns = ['input_column', 'output_column', 'message', 'clean_count']
        # df_pivot_report = df_pivot_report.sort_values(['out_column','clean_count'], ascending=False)
        df = df_pivot_report.copy()
        order = self.get_output_column(index)
        df['output_column'] = pd.Categorical(df['output_column'], categories=order, ordered=True)
        df_sorted = df.sort_values(by=['output_column', 'message'], ascending=False)
        return df_sorted
    
    def report(self, table_name):
        """_summary_

        Args:
            table_name (str): table name

        Returns:
            pd.DataFrame: report dataframe
        """
        df = self.output()
        op_len = len(df)
        
        # Get dirty report to calculate dirty values
        df_dirty_report = self.dirty_report()
        df_dirty_pivot = pd.pivot_table(
            df_dirty_report,
            values='dirty_count',
            index=['input_column'],
            aggfunc='sum',
        ).rename(columns={'dirty_count': 'count'})
    
        # Calculate dirty values, missing values, and clean values for each column
        dirty_df = df_dirty_pivot.reindex(index=df.columns)
        if df_dirty_pivot.empty:
            dirty_df['count'] = 0
        dirty_df = pd.Series(dirty_df['count']).fillna(0).astype(int)
        dirty_df.index.name = None
        dirty_data = dirty_df

        missing_df = df.isna().sum()
        missing_df = pd.Series(missing_df.to_frame(name='count')['count']).fillna(0).astype(int)
        missing_df.index.name = None
        missing_data = missing_df

        clean_data = op_len - (dirty_data + missing_data)

        # Calculate percentages
        dirty_data_percent = (dirty_data / op_len * 100).round(2)
        missing_data_percent = (missing_data / op_len * 100).round(2)
        clean_data_percent = (clean_data / op_len * 100).round(2)
        
        # Calculate completeness and consistency percentages
        complete_percent = (clean_data_percent + dirty_data_percent).round(2)
        # consis_percent = (clean_data_percent + missing_data_percent).round(2)

        # Create the resulting DataFrame
        check_df = pd.DataFrame({
            "Table": table_name,
            "Field": df.columns,
            "clean": clean_data,
            "dirty": dirty_data,
            "missing_data": missing_data,
            "clean_percent": clean_data_percent,
            "dirty_percent": dirty_data_percent,
            "missing_data_percent": missing_data_percent,
            "completeness_percent": complete_percent,
            # "consistency_percent": consis_percent
        })
        
        return check_df
    
    # def _generate_metrics_report(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    #     state = self.config.get("state")
    #     now = pd.Timestamp.now()

    #     if state == 'landing':
    #         # Count duplicates across all columns
    #         duplicate_count = df.duplicated().sum()
    #         return pd.DataFrame([{
    #             'name': table_name,
    #             'timestamp': now,
    #             'duplicate_count': duplicate_count
    #         }])

    #     elif state == 'staging':
    #         total_rows = len(df)
    #         report = {
    #             'name': table_name,
    #             'timestamp': now,
    #             'completeness': 100 * (1 - df.isnull().sum().mean() / total_rows),
    #             'validation': 100 * (df.ne("Failed").sum().mean() / total_rows),
    #             'consistency': 100 * (df.apply(lambda col: col.map(type).nunique() <= 1).mean()),
    #             'schema': 100.0  # You can plug in schema matching logic if needed
    #         }
    #         return pd.DataFrame([report])

    #     elif state == 'integration':
    #         # Example logic: assume you already know which cols should be deduped/joined
    #         duplicate_rate = 100 * (1 - df.duplicated().sum() / len(df))
    #         join_accuracy = 95.0  # Mocked value, adjust per real logic
    #         return pd.DataFrame([{
    #             'name': table_name,
    #             'timestamp': now,
    #             'duplicate_rate': duplicate_rate,
    #             'join_accuracy': join_accuracy
    #         }])

    #     else:
    #         raise Exception(f"Unsupported state for report: {state}")

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
            consistency_rules = metrics_config.get('consistency_rules', {})
            expected_schema = metrics_config.get('expected_schema', {})
            
            # Get validate score from report
            output_report = self.report(table_name)    
            validate_score = output_report.loc[df.columns]['clean']
            if not consistency_rules or not expected_schema:
                raise ValueError("Staging report requires 'consistency_rules' and 'expected_schema' in metrics_config")
            metrics = self.dq_framework.calculate_staging_zone_metrics(df, validate_score, consistency_rules, expected_schema)

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