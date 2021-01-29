"""
Common methods used by the Lambda function.
"""

import decimal
import json
from functools import wraps


class DecimalEncoder(json.JSONEncoder):
    """
    Encode a string to be stored in DynamoDB
    """

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def integration_tests(bypass):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if bypass:
                event = args[0]
                if 'integration_tests' in event and \
                        event['integration_tests'] == 'true':
                    return json.dumps({"result": True})
            return func(*args, **kwargs)

        return wrapper

    return decorator
