import sys
from sarc.jobs.node_gpu_mapping import _node_gpu_mapping_collection
from sarc.config import scraping_mode_required


@scraping_mode_required
def main():
    assert len(sys.argv) >= 3
    cluster_name, *nodes = sys.argv[1:]
    node_to_gpu: dict[str, set[str]] = {}
    for mapping in _node_gpu_mapping_collection().find_by(
        {"cluster_name": cluster_name}
    ):
        for node in nodes:
            if node in mapping.node_to_gpu:
                node_to_gpu.setdefault(node, set()).update(mapping.node_to_gpu[node])
    for node, gpus in node_to_gpu.items():
        print(f"{node}:", ", ".join(sorted(gpus)))


if __name__ == "__main__":
    main()
