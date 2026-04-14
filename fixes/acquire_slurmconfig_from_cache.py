import os
import subprocess

from sarc.config import config


def main():
    cfg = config()
    slurm_conf_folder = cfg.cache / "slurm_conf"
    for name in os.listdir(slurm_conf_folder):
        _, cluster, date, _ = name.split(".")
        command = [
            "uv",
            "run",
            "sarc",
            "-v",
            "acquire",
            "slurmconfig",
            "-c",
            cluster,
            "-d",
            date,
        ]
        print(f"[{' '.join(command)}]")
        result = subprocess.run(command, check=False)
        if result.returncode:
            print(f"[EXITED WITH ERROR CODE {result.returncode}] {' '.join(command)}")
        print()


if __name__ == "__main__":
    main()
