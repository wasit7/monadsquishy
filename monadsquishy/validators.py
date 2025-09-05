from functools import wraps
from . import utils

class CustomException(Exception):
    def __init__(self, value, message):
        super().__init__(message)
        self.value = value

def completeness(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if result is not None:
            raise Exception(utils.status.NOT_MISSING)
        return None
    wrapper.decorator_name = "missing"
    return wrapper

def validity(func):
    @wraps(func)
    def wrapper(x, *args, **kwargs):
        try:
            result = func(x, *args, **kwargs)  # convention: returns x if valid, else None
        except Exception:
            return x
        
        if result is not None:
            raise Exception(utils.status.VALID)
        return x
    wrapper.decorator_name = "invalid"
    return wrapper

def consistency(func):
    @wraps(func)
    def wrapper(x, *args, **kwargs):
        try:
            result = func(x, *args, **kwargs)
        except Exception:
            raise CustomException(x, utils.status.INCONSISTENT)
        
        if result:
            return result
        raise CustomException(x, utils.status.INCONSISTENT)
    wrapper.decorator_name = "passed"
    return wrapper

def fix(func):
    @wraps(func)
    def wrapper(x, *args, **kwargs):
        try:
            result = func(x, *args, **kwargs)
        except Exception:
            wrapper.decorator_name = "not_fixed"
            raise CustomException(x, utils.status.NOT_FIXED)  

        if result is not None:
            wrapper.decorator_name = "fixed"
            raise CustomException(result, utils.status.FIXED)
        wrapper.decorator_name = "not_fixed"
        raise CustomException(x, utils.status.NOT_FIXED)
    return wrapper