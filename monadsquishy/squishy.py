import os
import json
import datetime
import pandas as pd
from tqdm.auto import tqdm

from . import utils

tqdm.pandas()
class Monad:
    def __init__(self, value):
        self.value = value
        self.status = 'dirty'  # Starting with 'dirty'
        self.message = []
        self.dtype = object

    def __or__(self, func):
        if self.status == 'dirty':  # Only process if 'dirty'
            try:
                x = func(self.value)
                self.value = x
                self.status = 'passed'
                self.message.append(f'Passed: {func.__name__}()')
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
        if monad.status == 'dirty':
            monad.value = None
        return monad

class Squishy:
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
            # 'status':df['monad_result'].apply(lambda x: x.status),
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
                df_all_transformed[out_col]=df_transformed['value'] # must be string
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
            df_all_transformed.to_parquet(os.path.join(path,'transformed.parquet'))

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
            # chain of thought here
        
    def run(self):
        return self.all_transformations(self.config)
        
    def input(self, index=0):
        return self.config['transformations'][index]['input_table']

    def output(self, index=0):
        path = self.config['transformations'][index]['transformed_path']
        return pd.read_parquet(os.path.join(path,'transformed.parquet'))

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
        df_not_passed = df_last[df_last['is_passed'] == False]

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
        df = df_pivot_report
        order = self.get_output_column(index)
        df['output_column'] = pd.Categorical(df['output_column'], categories=order, ordered=True)
        df_sorted = df.sort_values(by=['output_column','dirty_count'])
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
        df = df_pivot_report
        order = self.get_output_column(index)
        df['output_column'] = pd.Categorical(df['output_column'], categories=order, ordered=True)
        df_sorted = df.sort_values(by=['output_column', 'message'])
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
        
        # Calculate null values, missing values, and clean values for each column
        null_data = df.isna().sum()
        missing_data = (df == "MISSING_DATA").sum()
        clean_data = op_len - null_data - missing_data

        # Calculate percentages
        null_data_percent = (null_data / op_len * 100).round(2)
        missing_data_percent = (missing_data / op_len * 100).round(2)
        clean_data_percent = (clean_data / op_len * 100).round(2)
        
        # Calculate completeness and consistency percentages
        complete_percent = (clean_data_percent + null_data_percent).round(2)
        consis_percent = (clean_data_percent + missing_data_percent).round(2)

        # Create the resulting DataFrame
        check_df = pd.DataFrame({
            "Table": table_name,
            "Field": df.columns,
            "clean": clean_data,
            "dirty": null_data,
            "missing_data": missing_data,
            "clean_percent": clean_data_percent,
            "dirty_percent": null_data_percent,
            "missing_data_percent": missing_data_percent,
            "completeness_percent": complete_percent,
            "consistency_percent": consis_percent
        })
        
        return check_df
    
    def save(self, table_name):
        if getattr(self, 'bucket_config', None) == None:
            raise Exception("Please config `osd_config` before .save()")
        df_output = self.output()
        df_report = self.report(table_name=table_name)

        base_path = f"{self.bucket_config.get('bucket', '')}/{self.config.get('state')}"
        path = f"{base_path}/{table_name}_{datetime.datetime.now().strftime('%Y-%m-%d')}.parquet"
        print(f"\t save data to {path}")
        df_output.to_parquet(
            path,
            filesystem=self.bucket,
            engine='pyarrow'
        )
        print(f"\t save data done!")

        path = f"{base_path}/{table_name}_{datetime.datetime.now().strftime('%Y-%m-%d')}-report.json"
        print(f"\t save report to {path}")
        with self.bucket.open(path, 'w') as f:
            f.write(json.dumps(df_report.to_dict('records')))
        print(f"\t save report done!")