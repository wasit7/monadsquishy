import pandas as pd
from datetime import datetime as dt
import warnings
import re

def country1(x):
    # passed or raise error
    valid_countries = ['TH', 'US', 'UK']
    if x not in valid_countries:
        raise Exception("Invalid code")
    else:
        return x

def country2(x):
    valid_countries = {'Thailand': 'TH', 'United States': 'USA', 'United Kingdom': 'UK'}
    return valid_countries[x]

def country3(x):
    valid_countries = {'ไทย': 'TH', 'สหรัฐอเมริกา': 'USA', 'อังกฤษ': 'UK'}
    return valid_countries[x]


def date1(date):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return pd.to_datetime(date, dayfirst =True, errors='raise').strftime('%Y-%m-%d')

def date2(thai_date_str):
    # Replace Thai Buddhist year with Gregorian year
    gregorian_year = str(int(thai_date_str.split()[-1]) - 543)
    thai_date_str = thai_date_str.replace(thai_date_str.split()[-1], gregorian_year)
    
    # Define the month mapping for Thai months
    thai_months = {
        "ม.ค.": "01",
        "ก.พ.": "02",
        "มี.ค.": "03",
        "เม.ย.": "04",
        "พ.ค.": "05",
        "มิ.ย.": "06",
        "ก.ค.": "07",
        "ส.ค.": "08",
        "ก.ย.": "09",
        "ต.ค.": "10",
        "พ.ย.": "11",
        "ธ.ค.": "12"
    }
    
    # Replace Thai month abbreviation with numeric month
    for thai_month, numeric_month in thai_months.items():
        thai_date_str = thai_date_str.replace(thai_month, numeric_month)
    
    # Parse the date string to a datetime object
    return dt.strptime(thai_date_str, "%d %m %Y").strftime("%Y-%m-%d")


def quantity1(quantity):
    match = re.search(r'\d+', str(quantity))
    if match:
        return int(match.group())
    else:
        raise Exception("Invalid quantity")

def price1(price_str):
    # Regular expression to match price patterns (handles both commas and periods)
    price_str = re.sub(r'[^\d.,]', '', price_str)  # Remove any non-numeric characters (except for commas and periods)
    price_str = price_str.replace(',', '')  # Replace comma with a period
    return float(price_str)

def currency1(price_str):
    # Define a regular expression for known ISO currency codes and Thai Baht
    currency_patterns = {
        r'\$': 'USD',
        r'USD': 'USD',
        r'THB': 'THB',
        r'บาท': 'THB'
    }
    
    # Loop through patterns and check for matches
    for pattern, iso_code in currency_patterns.items():
        if re.search(pattern, price_str):
            return iso_code
    
    # Default case raise error
    raise Exception("Invalid quantity")