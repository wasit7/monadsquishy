from functools import wraps
from .. import utils

def completeness(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if not result or result == '':
            return None
        raise Exception(utils.status.NOT_MISSING)
    return wrapper

def fix(func):
    @wraps(func)
    def wrapper(x, *args, **kwargs):
        result = func(x, *args, **kwargs)
        if result:
            return result
        raise Exception(utils.status.NOT_FIXED)
    return wrapper

def validity(func):
    @wraps(func)
    def wrapper(x, *args, **kwargs):
        result = func(x, *args, **kwargs)  # convention: returns x if valid, else None
        if result:
            raise Exception(utils.status.VALID)
        return x
    return wrapper

def consistency(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if result:
            return result
        raise Exception(utils.status.INCONSISTENT)
    return wrapper