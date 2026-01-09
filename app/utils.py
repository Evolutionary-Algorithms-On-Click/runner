import json

def parse_json_string(json_string):
    """Parses a JSON string and returns a Python object."""
    try:
        data = json.loads(json_string)
        return data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None
