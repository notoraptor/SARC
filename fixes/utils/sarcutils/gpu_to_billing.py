import pprint
import sys

from sarc.client import get_cluster_gpu_billings


def main():
    cluster_name = sys.argv[1]
    billings = [b.model_dump() for b in get_cluster_gpu_billings(cluster_name)]
    pprint.pprint(billings)


if __name__ == "__main__":
    main()
