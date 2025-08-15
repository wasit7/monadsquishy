MISSING = "missing"
NOT_MISSING = "not_missing"
FIXED = "fix"
NOT_FIXED = 'not_fixed'
VALID = "valid"
INVALID = "invalid"
INCONSISTENT = "inconsistent"
PASS = "passed"

pass_status = [
    MISSING,
    FIXED,
    INVALID,
    PASS,
]

fail_status = [
    NOT_MISSING,
    NOT_FIXED,
    VALID,
    INCONSISTENT
]