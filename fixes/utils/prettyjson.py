import json
import sys


def main():
    path = sys.argv[1]
    with open(path, "r", encoding="utf-8") as f:
        content = json.load(f)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(content, f, indent=1, allow_nan=True)


if __name__ == "__main__":
    main()
