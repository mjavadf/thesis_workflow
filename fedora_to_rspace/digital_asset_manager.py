#!/usr/bin/env python3
"""Image Downloader & Converter – Fedora → ResearchSpace
=======================================================

CLI tool that reads a list of Fedora NonRDF resource URIs, downloads the
binary content, converts it to JPG when needed, and stores it into a
ResearchSpace-aligned folder tree.

Example usage
```powershell
python download_images.py `
  --files sparql_out\files.txt `
  --out-dir "E:\\Workspace\\Ficlit-ETL\\researchspace-docker\\researchspace\\data\\images\\file" `
  --username [username] `
  --password [password] `
  --workers 5

```

"""

import argparse
import os
import sys
from pathlib import Path
from io import BytesIO
import requests
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed


def download_and_convert(url, out_dir, auth):
    base_prefix = "http://datavault.ficlit.unibo.it/repo/rest/"
    if not url.startswith(base_prefix):
        raise ValueError(f"Unexpected URL format: {url}")

    rel_path = url[len(base_prefix):]
    resource_root, filename = os.path.split(rel_path)
    filename_jpg = os.path.splitext(filename)[0] + ".jpg"
    target_dir = Path(out_dir) / resource_root
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / filename_jpg

    r = requests.get(url, auth=auth, stream=True, timeout=60)
    r.raise_for_status()

    with BytesIO(r.content) as buf:
        try:
            img = Image.open(buf)
            rgb = img.convert("RGB")
            rgb.save(target_path, format="JPEG")
        except Exception:
            with open(target_path, "wb") as f:
                f.write(r.content)

    return target_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", required=True, help="Path to files.txt")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--username", required=True, help="Fedora username")
    ap.add_argument("--password", required=True, help="Fedora password")
    ap.add_argument("--workers", type=int, default=5, help="Number of concurrent workers")
    args = ap.parse_args()

    auth = (args.username, args.password)
    out_dir = Path(args.out_dir)

    with open(args.files, encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_url = {executor.submit(download_and_convert, url, out_dir, auth): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                path = future.result()
                print(f"✓ Saved {path}")
            except Exception as e:
                print(f"✗ Failed {url}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
