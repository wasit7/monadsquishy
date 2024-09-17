import os
import pandas as pd

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
        df=df[[column_name]].copy()
        df['input']=df[column_name]
        df['monad_result'] = df[column_name].apply(lambda x: Monad(x).apply(monad_funcs))
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
        if not os.path.exists(path): 
            os.makedirs(path) 

    def all_transformations(self, sq_config):
        all_transformed=[]
        all_exploded=[]
        for pull in sq_config['transformations']:
            df_all_transformed=pd.DataFrame()
            df_all_exploded=pd.DataFrame()
            _df = pull['input_table']
            for k,v in pull['out_columns'].items():
                out_col = k
                in_col = v['input']
                funcs = v['funcs']
                print(f'''input:{in_col[:10]:10} -->{str([ f.__name__ for f in funcs]):40}--> output:{out_col}''')
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
                    "out_column": "out_column", 
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
            df_all_exploded['input_value']=df_all_exploded['input_value'].astype(str)
            df_all_exploded['output_value']=df_all_exploded['output_value'].astype(str)
            df_all_exploded.to_parquet(os.path.join(path,'exploded.parquet'))

            # append the global list
            all_transformed.append(df_all_transformed)
            all_exploded.append(df_all_exploded)
        return all_transformed, all_exploded
        
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

