# Squishy
<img src="https://github.com/wasit7/monadsquishy/blob/main/assets/logo.png" alt="Logo" width="200">

Welcome to the project!
Squishy is a Python package for data transformation using the Monad pattern and cleansing operations.

## Installation

### From PyPI:

```bash
pip install monadsquishy
```

### From github
```bash
pip install git+https://github.com/wasit7/monadsquishy.git
```

## Simple Transformation

Please check tutorial notebook [here](./tutorials/simple_transform.ipynb)

### load input into a dataframe
``` python
from monadsquishy import Squishy, sf
import pandas as pd

df = pd.read_parquet('./simple.parquet')
```

### Setup transformation cofig
```python
sq_config = {
    'transformations':[
        {
            'input_table': df,
            'transformed_path':'./staging/test1',
            'exploded_path':'./staging/test1',
            'out_columns': {
                 'country': {
                     'input':'country',
                     'funcs':[sf.country1,sf.country2,sf.country3]
                 },
                 'name': {
                     'input':'name',
                     'funcs':[lambda x:x, ],
                 },
                 'order_date': {
                     'input':'order_date',
                     'funcs':[sf.date1, sf.date2]
                 },
                 'quantity': {
                     'input':'quantity',
                     'funcs':[sf.quantity1, ]
                 },
                 'price_number': {
                     'input':'price',
                     'funcs':[sf.price1, ]
                 },
                 'price_currency': {
                     'input':'price',
                     'funcs':[sf.currency1, ]
                 } 
             }
        }
    ]
}
```

### Run transformations


```python
sq=Squishy(sq_config)
sq.run()
```

### Show input dataframe
```python
sq.input()
```

| country         | name         | order_date     | quantity   | price           |
|-----------------|--------------|----------------|------------|-----------------|
| TH              | มานี         | 9/25/2024      | 5 ชิ้น      | $25.99          |
| United States   | John Smith   | 20240926       | 7 units     | 30.00 USD       |
| ไทย             | ปิติ         | 27-09-2024     | pcs: 4      | 1,025.99 THB    |
| อังกฤษ          | Smith, J.    | Sep 28, 2024   | 10          | 1,000,025.99 บาท|
| invalid name    | J.S.         | 29 ก.ย. 2567   | 5pc         | ๒๗.๙๙ บาท      |


### Show output dataframe
```python
sq.output()
```

| country | name        | order_date  | quantity | price_number | price_currency |
|---------|-------------|-------------|----------|--------------|----------------|
| TH      | มานี        | 2024-09-25  | 5        | 25.99        | USD            |
| USA     | John Smith  | 2024-09-26  | 7        | 30.00        | USD            |
| TH      | ปิติ        | 2024-09-27  | 4        | 1025.99      | THB            |
| UK      | Smith, J.   | 2024-09-28  | 10       | 1000025.99   | THB            |
|         | J.S.        | 2024-09-29  | 5        | 27.99        | THB            |


### Show log dataframe
```python
sq.log()
```

| input_row | input_column | input_value      | output_value  | is_passed | message                                      |
|-----------|--------------|------------------|---------------|-----------|----------------------------------------------|
| 0         | country       | TH               | TH            | True      | Passed: country1()                           |
| 1         | country       | United States    | USA           | False     | Failed: country1(): Invalid code             |
| 2         | country       | United States    | USA           | True      | Passed: country2()                           |
| 3         | country       | ไทย              | TH            | False     | Failed: country1(): Invalid code             |
| 4         | country       | ไทย              | TH            | False     | Failed: country2(): 'ไทย'                    |
| 5         | country       | ไทย              | TH            | True      | Passed: country3()                           |
| 6         | country       | อังกฤษ           | UK            | False     | Failed: country1(): Invalid code             |
| 7         | country       | อังกฤษ           | UK            | False     | Failed: country2(): 'อังกฤษ'                 |
| 8         | country       | อังกฤษ           | UK            | True      | Passed: country3()                           |
| 9         | country       | invalid name     |               | False     | Failed: country1(): Invalid code             |
| 10        | country       | invalid name     |               | False     | Failed: country2(): 'invalid name'           |
| 11        | country       | invalid name     |               | False     | Failed: country3(): 'invalid name'           |
| 12        | name          | มานี             | มานี          | True      | Passed: <lambda>()                           |
| 13        | name          | John Smith       | John Smith    | True      | Passed: <lambda>()                           |
| 14        | name          | ปิติ              | ปิติ           | True      | Passed: <lambda>()                           |
| 15        | name          | Smith, J.        | Smith, J.     | True      | Passed: <lambda>()                           |
| 16        | name          | J.S.             | J.S.          | True      | Passed: <lambda>()                           |
| 17        | order_date    | 9/25/2024        | 2024-09-25    | True      | Passed: date1()                              |
| 18        | order_date    | 20240926         | 2024-09-26    | True      | Passed: date1()                              |
| 19        | order_date    | 27-09-2024       | 2024-09-27    | True      | Passed: date1()                              |
| 20        | order_date    | Sep 28, 2024     | 2024-09-28    | True      | Passed: date1()                              |
| 21        | order_date    | 29 ก.ย. 2567      | 2024-09-29    | False     | Failed: date1(): Unknown datetime string...  |
| 22        | order_date    | 29 ก.ย. 2567      | 2024-09-29    | True      | Passed: date2()                              |
| 23        | quantity      | 5 ชิ้น           | 5             | True      | Passed: quantity1()                          |
| 24        | quantity      | 7 units          | 7             | True      | Passed: quantity1()                          |
| 25        | quantity      | pcs: 4           | 4             | True      | Passed: quantity1()                          |
| 26        | quantity      | 10               | 10            | True      | Passed: quantity1()                          |
| 27        | quantity      | 5pc              | 5             | True      | Passed: quantity1()                          |
| 28        | price         | $25.99           | 25.99         | True      | Passed: price1()                             |
| 29        | price         | 30.00 USD        | 30.0          | True      | Passed: price1()                             |
| 30        | price         | 1,025.99 THB     | 1025.99       | True      | Passed: price1()                             |
| 31        | price         | 1,000,025.99 บาท | 1000025.99    | True      | Passed: price1()                             |
| 32        | price         | ๒๗.๙๙ บาท        | 27.99         | True      | Passed: price1()                             |
| 33        | price         | $25.99           | USD           | True      | Passed: currency1()                          |
| 34        | price         | 30.00 USD        | USD           | True      | Passed: currency1()                          |
| 35        | price         | 1,025.99 THB     | THB           | True      | Passed:


