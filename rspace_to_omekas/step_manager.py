# step_manager.py
import os
import json

from metadata_manager import run_query, build_sparql, load_rules, create_or_update_item
from digital_assets_manager import attach_media, convert_to_jpg

# Load config
paramsPath = "config_lab.json"
paramsJson = json.loads(open(paramsPath, "rb").read())
apiURL = paramsJson["apiURL"]
params = {
    "key_identity": paramsJson["key_identity"],
    "key_credential": paramsJson["key_credential"],
}
mediaRootDir = paramsJson["mediaRootDir"]

def main():
    rules = load_rules("rules_rs2os.yaml")
    query = build_sparql(rules)
    rows = run_query(query)

    for r in rows:
        uri = r["s"]["value"]
        fields = {}

        for f in rules["fields"]:
            prop = f["to"].get("property") if "to" in f else None
            var = f["select"]["as"].lstrip("?")
            val = r.get(var, {}).get("value")
            if val and prop:
                fields[prop] = val

        # Create or update item
        item_id = create_or_update_item(uri, fields)

        if item_id:
            for f in rules["fields"]:
                special = f["to"].get("special") if "to" in f else None
                if special == "o:media":
                    var = f["select"]["as"].lstrip("?")
                    val = r.get(var, {}).get("value")
                    if val:
                        for p in val.split("||"):
                            fullpath = os.path.join(mediaRootDir, p.replace("!", os.sep))
                            jpg_path = convert_to_jpg(fullpath)
                            if jpg_path:
                                attach_media(apiURL, params, item_id, jpg_path, fields.get("dcterms:title"))

    print("Done.")

if __name__ == "__main__":
    main()
