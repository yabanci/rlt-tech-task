import os
import pymongo
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
DB_URL = os.getenv("DB_URL")

client = pymongo.MongoClient(DB_URL)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

def aggregate_payments(dt_from, dt_upto, group_type):
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    pipeline = []

    if group_type == "hour":
        pipeline = [
            {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
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
            {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
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
            {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
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
    dataset = []
    labels = []

    label_index = {entry["_id"]: idx for idx, entry in enumerate(result)}

    current_date = dt_from
    while current_date < dt_upto:
        formatted_date = current_date.strftime("%Y-%m-%dT%H:00:00" if group_type == "hour" else "%Y-%m-%dT00:00:00")
        labels.append(formatted_date)
        if formatted_date in label_index:
            dataset.append(result[label_index[formatted_date]]["total"])
        else:
            dataset.append(0)
        current_date = current_date + timedelta(hours=1) if group_type == "hour" else current_date + timedelta(days=1)

    return {"dataset": dataset, "labels": labels}

if __name__ == "__main__":
    dt_from = "2022-10-01T00:00:00"
    dt_upto = "2022-11-30T23:59:00"
    group_type = "day"

    result = aggregate_payments(dt_from, dt_upto, group_type)
    print(result)
