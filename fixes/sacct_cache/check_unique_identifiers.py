import sys

from collections import Counter

from fixes.sacct_cache.job_state_classifier import EPOCHS, get_states_classifier
from sarc.client.job import SlurmState


def main():
    output_path = sys.argv[1] if len(sys.argv) == 2 else None
    if output_path:
        output = open(output_path, mode="w", encoding="utf-8")
        print("Output in:", output_path)
    else:
        output = sys.stdout

    try:
        print("EPOCHS:", file=output)
        for cluster_name, epoch_limit in EPOCHS.items():
            print(f"\t{cluster_name}\t{epoch_limit}", file=output)
        print(file=output)

        classifier = get_states_classifier()

        key_requeued = SlurmState.REQUEUED.name
        for cluster_name, cluster_epoch in classifier.items():
            for epoch, job_indices in cluster_epoch.items():
                for job_id, submit_times in job_indices.items():
                    if len(submit_times) > 1:
                        # Many submit times for a same couple (cluster_name, job_id) in an epoch
                        # Check if first states for all entries are: (many requeued) + (1 non-requeued)
                        states_count = Counter(
                            states[0] for states in submit_times.values()
                        )
                        states_count.pop(key_requeued, None)
                        if states_count.total() != 1:
                            # Excluding requeued, there are still many non-requeued jobs. Strange, but let's see.
                            print(cluster_name, job_id, file=output)
                            for submit_time, job_states in sorted(submit_times.items()):
                                print(
                                    f"\t{submit_time}\t{', '.join(job_states)}",
                                    file=output,
                                )
    finally:
        if output_path:
            output.close()


if __name__ == "__main__":
    main()
