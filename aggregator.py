import os
import pymongo
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv

load_dotenv()

DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
DB_URL = os.getenv("DB_URL")

client = pymongo.MongoClient(DB_URL)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

def aggregate_payments(dt_from, dt_upto, group_type):
    dt_from = datetime.fromisoformat(dt_from).replace(tzinfo=pytz.UTC)
    dt_upto = datetime.fromisoformat(dt_upto).replace(tzinfo=pytz.UTC)
    
    if group_type == 'hour':
        group_by = {
            '$dateToString': {
                'format': '%Y-%m-%dT%H:00:00',
                'date': '$timestamp'
            }
        }
        interval = timedelta(hours=1)
    elif group_type == 'day':
        group_by = {
            '$dateToString': {
                'format': '%Y-%m-%dT00:00:00',
                'date': '$timestamp'
            }
        }
        interval = timedelta(days=1)
    elif group_type == 'month':
        group_by = {
            '$dateToString': {
                'format': '%Y-%m-01T00:00:00',
                'date': '$timestamp'
            }
        }
        interval = timedelta(days=30)  # approximate
    else:
        raise ValueError("Invalid group_type")

    pipeline = [
        {'$match': {'timestamp': {'$gte': dt_from, '$lt': dt_upto}}},
        {'$group': {'_id': group_by, 'total': {'$sum': '$amount'}}},
        {'$sort': {'_id': 1}}
    ]
    
    result = list(collection.aggregate(pipeline))
    
    dataset = [r['total'] for r in result]
    labels = [r['_id'] for r in result]
    
    return {'dataset': dataset, 'labels': labels}
