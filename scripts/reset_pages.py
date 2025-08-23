#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from data.model import JobBoard, list_page_files


def reset_pages(pages_dir: Path, backup: bool = False) -> int:
    pages_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for jf in list_page_files(pages_dir):
        try:
            jb = JobBoard.from_file(jf)
        except Exception as e:
            print(f"Skipping {jf}: {e!r}")
            continue

        jb.content = []
        jb.last_scraped = None
        jb.next_scrape_at = None

        if backup:
            shutil.copy2(jf, jf.with_suffix(jf.suffix + ".bak"))

        jb.to_file(jf)
        print(f"Reset {jf}")
        n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description="Reset content & scraped dates for all page JSONs.")
    ap.add_argument("--pages-dir", type=Path, default=Path("data/pages"), help="Directory with *.json pages")
    ap.add_argument("--backup", action="store_true", help="Write .bak copies before modifying")
    args = ap.parse_args()

    total = reset_pages(args.pages_dir, backup=args.backup)
    print(f"Done. Reset {total} page(s).")


if __name__ == "__main__":
    main()
