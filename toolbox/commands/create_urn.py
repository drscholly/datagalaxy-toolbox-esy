import json
import logging
from typing import Optional

from toolbox.api.datagalaxy_api_modules import DataGalaxyApiModules
from toolbox.api.http_client import HttpClient
from toolbox.commands.utils import config_workspace


def create_urn(url: str,
               token: str,
               workspace_name: Optional[str],
               file_path: str,
               http_client: HttpClient) -> int:

    workspace = config_workspace(
        mode="target",
        url=url,
        token=token,
        workspace_name=workspace_name,
        version_name=None,
        http_client=http_client
    )
    if not workspace:
        return 1

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            objects = json.load(f)
    except Exception as e:
        logging.error(f"Failed to read URN objects file '{file_path}': {e}")
        return 1

    module_api = DataGalaxyApiModules(
        url=url,
        token=token,
        workspace=workspace,
        module="URN",
        http_client=http_client
    )

    module_api.create_urn(objects)
    return 0


def create_urn_parse(subparsers):
    # create the parser for the "create-urn" command
    create_urn_parse = subparsers.add_parser('create-urn', help='create-urn help')
    create_urn_parse.add_argument(
        '--url',
        type=str,
        help='url source',
        required=True)
    create_urn_parse.add_argument(
        '--token',
        type=str,
        help='token',
        required=True)
    create_urn_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace name',
        required=False)
    create_urn_parse.add_argument(
        '--file',
        type=str,
        default='urn_objects.json',
        help='path to JSON file containing URN objects and links (default: urn_objects.json)',
        required=False)
