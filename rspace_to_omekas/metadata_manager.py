# metadata_manager.py
import json
import yaml
import requests
from SPARQLWrapper import SPARQLWrapper, JSON

# Load config
paramsPath = "config_lab.json"
paramsJson = json.loads(open(paramsPath, "rb").read())
apiURL = paramsJson["apiURL"]
params = {
    "key_identity": paramsJson["key_identity"],
    "key_credential": paramsJson["key_credential"],
}
headers = {"Content-type": "application/json"}
sparql_endpoint = paramsJson["sparqlEndpoint"]

# ---------------- SPARQL helpers ----------------
def run_query(query: str):
    sparql = SPARQLWrapper(sparql_endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setMethod("POST")
    sparql.setQuery(query)
    result = sparql.query().convert()
    # Handle the result properly by ensuring it's a dictionary
    if isinstance(result, dict) and "results" in result and "bindings" in result["results"]:
        return result["results"]["bindings"]
    return []

def build_sparql(rules: dict) -> str:
    px = rules.get("prefixes", {})
    pfx = "\n".join([f"PREFIX {k}: <{v}>" for k, v in px.items()])

    root = rules["root"]
    s = root["subject_var"]
    cls = root["class"]
    order_by = root.get("order_by", s)

    select_parts = [s]
    where_parts = [f"{s} a {cls} ."]

    for f in rules["fields"]:
        sel = f["select"]
        select_parts.append(f"({sel['expr']} AS {sel['as']})")

        if "where" in f and f["where"]:
            wtriples = "\n    ".join(f["where"])
            if f.get("required", False):
                where_parts.append(wtriples)
            else:
                where_parts.append(f"OPTIONAL {{\n    {wtriples}\n}}")

    select_line = " ".join(select_parts)
    joined_where = "\n  ".join(where_parts)

    query = f"""{pfx}

SELECT {select_line}
WHERE {{
  {joined_where}
}}
GROUP BY {s}
ORDER BY {order_by}
"""
    return query

def load_rules(path="rules_rs2os.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

# ---------------- Omeka helpers ----------------
def find_item_by_identifier(uri):
    check_url = f"{apiURL}items"
    query_params = {
        "key_identity": params["key_identity"],
        "key_credential": params["key_credential"],
        "property[0][property]": 10,
        "property[0][type]": "eq",
        "property[0][text]": uri,
    }
    resp = requests.get(check_url, params=query_params, headers=headers, verify=False)
    try:
        items = resp.json()
    except Exception:
        return None
    return items[0] if isinstance(items, list) and len(items) > 0 else None

def create_or_update_item(uri, fields):
    """Insert or update an item in OmekaS with given metadata {prop: value}."""
    property_ids = {
        "dcterms:title": 1,
        "dcterms:creator": 2,
        "dcterms:subject": 3,
        "dcterms:description": 4,
        "dcterms:publisher": 5,
        "dcterms:contributor": 6,
        "dcterms:date": 7,
        "dcterms:type": 8,
        "dcterms:format": 9,
        "dcterms:identifier": 10,
        "dcterms:source": 11,
        "dcterms:language": 12,
        "dcterms:relation": 13,
        "dcterms:coverage": 14,
        "dcterms:rights": 15,
    }

    data = {
        "dcterms:identifier": [{
            "type": "literal",
            "property_id": 10,
            "property_label": "Identifier",
            "is_public": True,
            "@value": uri,
        }],
        "@type": ["o:Item", "dctype:PhysicalObject"],
        "o:is_public": True,
        "o:resource_class": {"@id": f"{apiURL}resource_classes/32", "o:id": 32},
        "o:item_set": [{"@id": f"{apiURL}item_sets/2", "o:id": 2}],
    }

    for prop, value in fields.items():
        if prop == "dcterms:identifier":
            continue
        prop_id = property_ids.get(prop)
        if not prop_id:
            print(f"Warning: Unknown property {prop}, skipping")
            continue
        data[prop] = [{
            "type": "literal",
            "property_id": prop_id,
            "property_label": prop.split(":")[1].capitalize(),
            "is_public": True,
            "@value": value,
        }]

    existing = find_item_by_identifier(uri)
    if existing:
        item_id = existing["o:id"]
        url = f"{apiURL}items/{item_id}"
        response = requests.patch(url, params=params, data=json.dumps(data), headers=headers, verify=False)
        if response.status_code in (200, 201):
            print(f"Updated item {item_id} ({fields.get('dcterms:title', uri)})")
            return item_id, "updated"
        else:
            print(f"Error updating {uri}: {response.status_code} - {response.text}")
            return None, None
    else:
        response = requests.post(f"{apiURL}items", params=params, data=json.dumps(data), headers=headers, verify=False)
        if response.status_code in (200, 201):
            item_id = response.json().get("o:id")
            print(f"Created item {item_id} ({fields.get('dcterms:title', uri)})")
            return item_id, "created"
        else:
            print(f"Error creating {uri}: {response.status_code} - {response.text}")
            return None, None