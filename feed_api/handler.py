import json
from mangum import Mangum
from app import app

handler = Mangum(app)


def lambda_handler(event, context):
    return handler(event, context)
