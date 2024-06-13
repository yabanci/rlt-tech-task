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

    pipeline = []

    if group_type == "hour":
        pipeline = [
            {"$match": {"dt": {"$gte": dt_from, "$lt": dt_upto}}},
            {"$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:00:00",
                        "date": "$dt"
                    }
                },
                "total": {"$sum": "$value"}
            }},
            {"$sort": {"_id": 1}}
        ]
    elif group_type == "day":
        pipeline = [
            {"$match": {"dt": {"$gte": dt_from, "$lt": dt_upto}}},
            {"$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT00:00:00",
                        "date": "$dt"
                    }
                },
                "total": {"$sum": "$value"}
            }},
            {"$sort": {"_id": 1}}
        ]
    elif group_type == "month":
        pipeline = [
            {"$match": {"dt": {"$gte": dt_from, "$lt": dt_upto}}},
            {"$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-01T00:00:00",
                        "date": "$dt"
                    }
                },
                "total": {"$sum": "$value"}
            }},
            {"$sort": {"_id": 1}}
        ]
    else:
        raise ValueError("Invalid group_type")

    result = list(collection.aggregate(pipeline))
    dataset = [entry["total"] for entry in result]
    labels = [entry["_id"] for entry in result]

    return {"dataset": dataset, "labels": labels}

if __name__ == "__main__":
    dt_from = "2022-09-01T00:00:00"
    dt_upto = "2022-12-31T23:59:00"
    group_type = "month"

    result = aggregate_payments(dt_from, dt_upto, group_type)
    print(result)
