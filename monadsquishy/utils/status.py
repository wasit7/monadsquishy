MISSING = "missing"
NOT_MISSING = "not_missing"
INVALID = "invalid"
VALID = "valid"
INCONSIST = "inconsistent"
PASS = "passed"

pass_status = [
    PASS,
    MISSING,
    INVALID
]

fail_status = [
    NOT_MISSING,
    VALID,
    INCONSIST
]

def valid(fix_value: None):
    if fix_value is None:
        raise Exception(VALID)
    else:
        raise Exception(VALID, fix_value)

def inconsist(fix_value: None):
    if fix_value is None:
        raise Exception(INCONSIST)
    else:
        raise Exception(INCONSIST, fix_value)

def not_missing():
    raise Exception(NOT_MISSING)