import psycopg2
import csv
from dotenv import load_dotenv
import os

load_dotenv()
karya_db = os.getenv("KARYA_DB")
karya_user = os.getenv("KARYA_USER")
karya_pass = os.getenv("KARYA_PASS")
karya_host = os.getenv("KARYA_HOST")
karya_port = os.getenv("KARYA_PORT")

# Connect to db
dbconn = psycopg2.connect(database=karya_db,
                          user=karya_user,
                          password=karya_pass,
                          host=karya_host,
                          port=karya_port)

cursor = dbconn.cursor()

# Task Name, Task ID, Sum of duration in hours
header = ["task_name", "task_id", "sum_of_duration"]


def convert_ms_to_hrs(duration_in_ms):
    hours = (duration_in_ms / (1000 * 60 * 60)) % 24
    return hours


with open("sum_duration_group_task.csv", "w+") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    # Fetch a set of task IDs
    task_id_cursor = cursor.execute("""
            SELECT DISTINCT task_id
            FROM microtask_assignment
            """)
    task_ids = cursor.fetchall()

    for task_id in task_ids:
        cursor.execute("""
            SELECT output, report
            FROM microtask_assignment
            WHERE task_id={}
        """.format(task_id[0]))
        output = cursor.fetchall()

        sum_of_duration = 0

        for out in output:
            if out[0] is not None and out[1] is not None:
                if "fraction" in out[1].keys() and out[1]["fraction"] == 1 and "duration" in out[0]["data"]:
                    if out[0]["data"]["duration"] >= 0 or out[0]["data"]["duration"] <= 90 * 1000:
                        sum_of_duration += out[0]["data"]["duration"]
                elif "accuracy" in out[1].keys() and out[1]["accuracy"] == 2 and "duration" in out[0]["data"]:
                    if out[0]["data"]["duration"] >= 0 or out[0]["data"]["duration"] <= 90 * 1000:
                        sum_of_duration += out[0]["data"]["duration"]

        if sum_of_duration != 0:
            task_name_cursor = cursor.execute("""
                    SELECT name
                    FROM task
                    WHERE id={}
                    """.format(task_id[0]))
            task_names = cursor.fetchall()
            sum_of_duration = convert_ms_to_hrs(sum_of_duration)
            final_list = [task_names[0][0], task_id[0], sum_of_duration]
            writer.writerow(final_list)

dbconn.close()
