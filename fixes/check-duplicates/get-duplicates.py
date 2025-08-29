import sys

from sarc.client import get_jobs
from sarc.config import config, scraping_mode_required, MTL
from datetime import datetime, timedelta
from sarc.client.job import SlurmJob, _jobs_collection, SlurmState
from sarc.jobs.node_gpu_mapping import get_node_to_gpu
from tqdm import tqdm
import pickle


def main():
    db_jobs = config().mongo.database_instance.jobs

    pipeline = [
        {
            "$group": {
                "_id": {
                    "cluster_name": "$cluster_name",
                    "job_id": "$job_id",
                },
                "count": {"$sum": 1},
                "items": {
                    "$push": {
                        "cluster_name": "$cluster_name",
                        "job_id": "$job_id",
                        "job_state": "$job_state",
                        "submit_time": "$submit_time",
                        "requested": "$requested.gres_gpu",
                        "allocated": "$allocated.gres_gpu",
                        "gpu_type": "$allocated.gpu_type",
                    }
                },
            }
        },
        {"$match": {"count": {"$gt": 1}}},
    ]

    print("Counting ...")
    pipeline_count = pipeline + [{"$count": "count"}]
    (result,) = db_jobs.aggregate(pipeline_count)
    count = result["count"]
    print("Count:", count)

    documents = [
        doc["items"]
        for doc in tqdm(db_jobs.aggregate(pipeline), total=count, desc="duplicates")
    ]
    with open("duplicates.mongodoc.pickle", mode="wb") as file:
        pickle.dump(documents, file)
    print("Dumped.")


if __name__ == "__main__":
    main()
