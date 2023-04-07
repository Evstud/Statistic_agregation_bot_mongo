from motor.motor_asyncio import AsyncIOMotorClient
from operator import itemgetter
from dateutil.relativedelta import *
import datetime
import asyncio


client = AsyncIOMotorClient()


async def get_db(client, db_name, collection_name):
    collection = client[db_name].get_collection(collection_name)
    return collection


async def get_data_by_month(collection, dt_from, task_id):
    try:
        sum_sum = await collection.aggregate([
            {"$match": {"dt": {"$gte": datetime.datetime.fromisoformat(dt_from),
                               "$lte": datetime.datetime.fromisoformat(dt_from) + relativedelta(months=1)}}},
            {"$group": {"_id": "$cust_id", "value": {"$sum": "$value"}}}
        ], allowDiskUse=True).to_list(None)
        try:
            sum_sum_add = sum_sum[0]['value']
        except:
            sum_sum_add = 0
    except Exception as first_ex:
        print("First_ex", first_ex)
        sum_sum_add = 0
    return {"sum_to_add": sum_sum_add, "task_id": task_id}


async def get_data_by_week(collection, dt_from, task_id):
    try:
        sum_sum = await collection.aggregate([
            {"$match": {"dt": {"$gte": datetime.datetime.fromisoformat(dt_from),
                               "$lt": datetime.datetime.fromisoformat(dt_from) + relativedelta(weeks=1)}}},
            {"$group": {"_id": "$cust_id", "value": {"$sum": "$value"}}}
        ]).to_list(None)
        try:
            sum_sum_add = sum_sum[0]['value']
        except:
            sum_sum_add = 0
    except Exception as first_ex:
        print("First_ex", first_ex)
        sum_sum_add = 0
    return {"sum_to_add": sum_sum_add, "task_id": task_id}


async def get_data_by_day(collection, dt_from, task_id):
    try:
        sum_sum = await collection.aggregate([
            {"$match": {"dt": {"$gte": datetime.datetime.fromisoformat(dt_from),
                               "$lt": datetime.datetime.fromisoformat(dt_from) + datetime.timedelta(days=1)}}},
            {"$group": {"_id": "$cust_id", "value": {"$sum": "$value"}}}
        ]).to_list(None)
        try:
            sum_sum_add = sum_sum[0]['value']
        except:
            sum_sum_add = 0
    except Exception as first_ex:
        print("First_ex", first_ex)
        sum_sum_add = 0
    return {"sum_to_add": sum_sum_add, "task_id": task_id}


async def get_data_by_hour(collection, dt_from, task_id):
    try:
        sum_sum = await collection.aggregate([
            {"$match": {"dt": {"$gte": datetime.datetime.fromisoformat(dt_from),
                               "$lt": datetime.datetime.fromisoformat(dt_from) + datetime.timedelta(hours=1)}}},
            {"$group": {"_id": "$cust_id", "value": {"$sum": "$value"}}}
        ]).to_list(None)
        try:
            sum_sum_add = sum_sum[0]['value']
        except:
            sum_sum_add = 0
    except Exception as first_ex:
        print("First_ex", first_ex)
        sum_sum_add = 0
    return {"sum_to_add": sum_sum_add, "task_id": task_id}


async def get_data(collection, dt_from, dt_upto, group_type):
    labels = []
    tasks = []
    t_id = 0
    while datetime.datetime.fromisoformat(dt_from) <= datetime.datetime.fromisoformat(dt_upto):
        if group_type == "month":
            t_id += 1
            tasks.append(asyncio.create_task(get_data_by_month(collection=collection, dt_from=dt_from, task_id=t_id)))
            labels.append(datetime.datetime.isoformat(
                datetime.datetime(datetime.datetime.fromisoformat(dt_from).year,
                                  datetime.datetime.fromisoformat(dt_from).month,
                                  datetime.datetime.fromisoformat(dt_from).day, 0, 0, 0)))
            dt_from = str((datetime.datetime.fromisoformat(dt_from) + relativedelta(months=1)))
        elif group_type == "week":
            t_id += 1
            tasks.append(asyncio.create_task(get_data_by_week(collection=collection, dt_from=dt_from, task_id=t_id)))
            labels.append(datetime.datetime.isoformat(
                datetime.datetime(datetime.datetime.fromisoformat(dt_from).year,
                                  datetime.datetime.fromisoformat(dt_from).month,
                                  datetime.datetime.fromisoformat(dt_from).day, 0, 0, 0)))
            dt_from = str((datetime.datetime.fromisoformat(dt_from) + relativedelta(weeks=1)))
        elif group_type == "day":
            t_id += 1
            tasks.append(asyncio.create_task(get_data_by_day(collection=collection, dt_from=dt_from, task_id=t_id)))
            labels.append(datetime.datetime.isoformat(
                datetime.datetime(datetime.datetime.fromisoformat(dt_from).year,
                                  datetime.datetime.fromisoformat(dt_from).month,
                                  datetime.datetime.fromisoformat(dt_from).day, 0, 0, 0)))
            dt_from = str((datetime.datetime.fromisoformat(dt_from) + datetime.timedelta(days=1)))
        elif group_type == "hour":
            t_id += 1
            tasks.append(asyncio.create_task(get_data_by_hour(collection=collection, dt_from=dt_from, task_id=t_id)))
            labels.append(datetime.datetime.isoformat(
                datetime.datetime(datetime.datetime.fromisoformat(dt_from).year,
                                  datetime.datetime.fromisoformat(dt_from).month,
                                  datetime.datetime.fromisoformat(dt_from).day,
                                  datetime.datetime.fromisoformat(dt_from).hour, 0, 0)))
            dt_from = str((datetime.datetime.fromisoformat(dt_from) + datetime.timedelta(hours=1)))

    dataset = await asyncio.gather(*tasks)
    list(dataset).sort(key=itemgetter('task_id'))
    dataset_val = []
    for da in dataset:
        dataset_val.append(da["sum_to_add"])
    if group_type == "hour":
        del dataset_val[-1]
        dataset_val.append(0)
    return {"dataset": dataset_val, "labels": labels}


async def main(client, db_name, collection_name, group_type, dt_from, dt_upto):
    return await get_data(
        collection=await get_db(client=client, db_name=db_name, collection_name=collection_name),
        group_type=group_type,
        dt_from=dt_from,
        dt_upto=dt_upto
    )
