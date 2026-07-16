from typing import Optional
import logging

from toolbox.api.datagalaxy_api_modules import DataGalaxyApiModules
from toolbox.api.http_client import HttpClient
from toolbox.commands.utils import config_workspace
from toolbox.api.dremio_api import extract_metadata


def scan_dremio(url: str,
                token: str,
                workspace_name: str,
                version_name: Optional[str],
                http_client: HttpClient) -> int:
    # Workspace
    workspace = config_workspace(
        mode="target",
        url=url,
        token=token,
        workspace_name=workspace_name,
        version_name=version_name,
        http_client=http_client
    )
    if not workspace:
        return 1

    # Module
    module_api = DataGalaxyApiModules(
        url=url,
        token=token,
        workspace=workspace,
        module="Dictionary",
        http_client=http_client
    )

    metadata_list = extract_metadata()
    if not metadata_list:
        logging.warning("No metadata retrieved from Dremio.")
        return 0

    source = {
        'name': "DREMIO",
        'technicalName': "DREMIO",
        'type': 'Relational',
        'path': "\\DREMIO",
        'functionalPath': "\\DREMIO",
        'typePath': '\\Relational',
        'attributes': {
            'technologyCode': 'dremio'
        }
    }

    for obj in metadata_list:
        obj['path'] = "\\DREMIO" + obj['path']
        obj['functionalPath'] = "\\DREMIO" + obj['functionalPath']
        obj['typePath'] = "\\Relational" + obj['typePath']

    module_api.create_source(
        workspace_name=workspace_name,
        source=source
    )

    module_api.bulk_upsert_source_tree(
        workspace_name=workspace_name,
        source=source,
        objects=[metadata_list],
        tag_value=None
    )

    return 0


# Parsers
def scan_dremio_parse(subparsers):
    # create the parser for the "scan_dremio" command
    scan_dremio_parse = subparsers.add_parser('scan-dremio', help='scan-dremio help')
    scan_dremio_parse.add_argument(
        '--url',
        type=str,
        help='url environnement',
        required=True)
    scan_dremio_parse.add_argument(
        '--token',
        type=str,
        help='token environnement',
        required=True)
    scan_dremio_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace name',
        required=True)
    scan_dremio_parse.add_argument(
        '--version',
        type=str,
        help='version name')
