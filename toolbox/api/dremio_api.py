import os
import requests
import logging

PROJECT_ID = os.environ.get("DREMIO_PROJECT_ID")
TOKEN = os.environ.get("DREMIO_TOKEN")

BASE_URL = f"https://api.dremio.cloud/v0/projects/{PROJECT_ID}" if PROJECT_ID else ""

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
} if TOKEN else {}


def get_catalog_by_path(path_list=None):
    if not PROJECT_ID:
        logging.error("dremio_api - DREMIO_PROJECT_ID environment variable is not set")
        return None
    if not TOKEN:
        logging.error("dremio_api - DREMIO_TOKEN environment variable is not set")
        return None

    if not path_list:
        url = f"{BASE_URL}/catalog"
    else:
        path_string = "/".join(path_list)
        url = f"{BASE_URL}/catalog/by-path/{path_string}"

    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"dremio_api - Error on path {path_list}: {response.status_code}")
        return None


def extract_metadata():
    logging.info("dremio_api - Extracting Dremio Cloud metadata...")

    root = get_catalog_by_path()
    if not root or 'data' not in root:
        logging.error("dremio_api - Impossible to retrieve catalog")
        return []

    metadata = []

    for source in root['data']:
        current_path = source['path']
        item_name = current_path[0]

        type_path = ["Model"]
        metadata.append({
            'name': item_name,
            'technicalName': item_name,
            'type': 'Model',
            'path': '\\' + '\\'.join(current_path),
            'functionalPath': '\\' + '\\'.join(current_path),
            'typePath': '\\' + '\\'.join(type_path),
            'attributes': {}
        })

        explore_path(current_path, type_path, metadata)

    logging.info("dremio_api - Extraction complete")
    return metadata


def explore_path(current_path, type_path, metadata):
    folder_content = get_catalog_by_path(current_path)

    if not folder_content or 'children' not in folder_content:
        return

    for item in folder_content['children']:
        item_path = item['path']
        item_name = item_path[-1]

        if item.get('type') == 'CONTAINER' or item.get('containerType') in ['FOLDER', 'SPACE']:
            current_type_path = type_path + ['Model']
            metadata.append({
                'name': item_name,
                'technicalName': item_name,
                'type': 'Model',
                'path': '\\' + '\\'.join(item_path),
                'functionalPath': '\\' + '\\'.join(item_path),
                'typePath': '\\' + '\\'.join(current_type_path),
                'attributes': {}
            })

            explore_path(item_path, current_type_path, metadata)

        elif item.get('type') == 'DATASET':
            current_type_path = type_path + ['Table']
            metadata.append({
                'name': item_name,
                'technicalName': item_name,
                'type': 'Table',
                'path': '\\' + '\\'.join(item_path),
                'functionalPath': '\\' + '\\'.join(item_path),
                'typePath': '\\' + '\\'.join(current_type_path),
                'attributes': {}
            })

            dataset_detail = get_catalog_by_path(item_path)
            if dataset_detail and 'fields' in dataset_detail:
                for field in dataset_detail['fields']:
                    col_name = field['name']
                    col_type = field['type']['name']

                    column_path = item_path + [col_name]
                    column_type_path = current_type_path + ['Column']
                    metadata.append({
                        'name': col_name,
                        'technicalName': col_name,
                        'type': 'Column',
                        'path': '\\' + '\\'.join(column_path),
                        'functionalPath': '\\' + '\\'.join(column_path),
                        'typePath': '\\' + '\\'.join(column_type_path),
                        'attributes': {
                            'dataType': col_type
                        }
                    })
