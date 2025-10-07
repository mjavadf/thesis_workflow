import json
from pathlib import Path
import requests
from PIL import Image
import os

# config imported by caller (step_manager)
def convert_to_jpg(filepath):
    """Convert image to JPG if not already, returns new path."""
    path = Path(filepath)
    if path.suffix.lower() == ".jpg":
        return str(path)

    new_path = str(path.with_suffix(".jpg"))
    try:
        img = Image.open(path)
        rgb = img.convert("RGB")
        rgb.save(new_path, "JPEG")
        print(f"Converted {path.name} -> {Path(new_path).name}")
        return new_path
    except Exception as e:
        print(f"Conversion failed for {path}: {e}")
        return None

def attach_media(apiURL, params, item_id, filepath, title=None):
    """Attach media file to an Omeka item."""
    path = Path(filepath)
    if not path.is_file():
        print(f"File not found: {filepath}")
        return

    # --- check if item already has media
    try:
        check = requests.get(f"{apiURL}items/{item_id}?embed=media", params=params, verify=False)
        if check.status_code == 200:
            data = check.json()
            if "o:media" in data and len(data["o:media"]) > 0:
                print(f"✓ Skipping upload: item {item_id} already has media attached")
                return
    except Exception as e:
        print(f"   [Warning] Could not check media for item {item_id}: {e}")

    title = title or path.stem
    data_item = {
        "o:ingester": "upload",
        "file_index": 0,
        "o:item": {"o:id": item_id},
        "dcterms:title": [{
            "property_id": 1,
            "property_label": "Title",
            "@value": title,
            "type": "literal"
        }],
    }

    media_upload = [
        ("data", (None, json.dumps(data_item), "application/json")),
        ("file[0]", (path.name, open(path, "rb"), "image/jpeg")),
    ]

    response = requests.post(f"{apiURL}media", params=params, files=media_upload, verify=False)
    if response.status_code in (200, 201):
        print(f"   Media attached: {path.name}")
        return True
    else:
        print(f"   Error uploading {path.name}: {response.status_code}")
        return False
