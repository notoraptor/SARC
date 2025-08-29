import contextlib
import sys
from pathlib import Path


@contextlib.contextmanager
def get_output_file(filename: str | Path | None = None):
    if filename is None:
        filename = sys.argv[1] if len(sys.argv) == 2 else None
    if filename is None:
        output = sys.stdout
    else:
        output = open(filename, mode="w", encoding="utf-8")
        print("[output]", filename)
    try:
        yield output
    finally:
        if filename is not None:
            output.close()
