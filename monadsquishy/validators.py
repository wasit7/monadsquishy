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
        if result:
            raise Exception(utils.status.NOT_MISSING)
        return None
    wrapper.decorator_name = "missing"
    return wrapper

def validity(func):
    @wraps(func)
    def wrapper(x, *args, **kwargs):
        result = func(x, *args, **kwargs)  # convention: returns x if valid, else None
        if result:
            raise Exception(utils.status.VALID)
        return x
    wrapper.decorator_name = "invalid"
    return wrapper

def consistency(func):
    @wraps(func)
    def wrapper(x, *args, **kwargs):
        result = func(x, *args, **kwargs)
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
            raise CustomException(x, utils.status.NOT_FIXED)

        if result:
            raise CustomException(result, utils.status.FIXED)
        raise CustomException(x, utils.status.NOT_FIXED)
    return wrapper