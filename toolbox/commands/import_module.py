from typing import Optional
import json
import logging
from json import JSONDecodeError
from pathlib import Path

from toolbox.api.datagalaxy_api import build_bulktree, create_batches, remove_technology_code
from toolbox.api.datagalaxy_api_modules import DataGalaxyApiModules
from toolbox.api.http_client import HttpClient
from toolbox.commands.utils import config_workspace


def import_module(module: str,
                  url: str,
                  token: str,
                  workspace_name: str,
                  version_name: Optional[str],
                  file_path: str,
                  http_client: HttpClient,
                  bulktree: bool = False) -> int:
    # Read input file
    input_file = Path(file_path)
    try:
        with open(input_file, encoding="utf-8") as f:
            source_objects = json.load(f)
    except FileNotFoundError:
        logging.error(f'import-module - File not found: {input_file}')
        return 1
    except JSONDecodeError as error:
        logging.error(f'import-module - Invalid JSON file {input_file}: {error}')
        return 1

    if not isinstance(source_objects, list):
        logging.error(f'import-module - File {input_file} must contain a JSON list.')
        return 1

    if source_objects == [] or source_objects == [[]]:
        logging.warning(f'import-module - No object in file {input_file}, aborting.')
        return 1

    # Target workspace
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

    # Target module API
    module_api = DataGalaxyApiModules(
        url=url,
        token=token,
        workspace=workspace,
        module=module,
        http_client=http_client
    )

    if bulktree:
        # Bulktrees input
        batches = source_objects
    else:
        # Flat file input
        batches = create_batches([source_objects])
    for batch in batches:
        if module == "Links" or bulktree:
            bulktree_to_import = batch
        else:
            bulktree_to_import = build_bulktree(batch)
        # If a parent usage has a technology, children must not repeat technologyCode.
        # TODO remove this logic and put it in "build_bulktree"
        for tree in bulktree_to_import:
            if 'children' in tree:
                for child in tree['children']:
                    remove_technology_code(child)
        # Send bulktree on target workspace
        module_api.bulk_upsert_tree(
            workspace_name=workspace_name,
            bulktree=bulktree_to_import,
            tag_value=None
        )

    return 0


# Parsers
def import_glossary_parse(subparsers):
    # create the parser for the "import_glossary" command
    import_glossary_parse = subparsers.add_parser('import-glossary', help='import-glossary help')
    import_glossary_parse.add_argument(
        '--url',
        type=str,
        help='url target environnement',
        required=True)
    import_glossary_parse.add_argument(
        '--token',
        type=str,
        help='token target environnement',
        required=True)
    import_glossary_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace target name',
        required=True)
    import_glossary_parse.add_argument(
        '--version',
        type=str,
        help='version target name')
    import_glossary_parse.add_argument(
        '--file',
        type=str,
        help='path of the JSON file to import',
        required=True)
    import_glossary_parse.add_argument(
        '--bulktree',
        action='store_true',
        help='convert a flat JSON file to bulktree before import')


def import_dictionary_parse(subparsers):
    # create the parser for the "import_dictionary" command
    import_dictionary_parse = subparsers.add_parser('import-dictionary', help='import-dictionary help')
    import_dictionary_parse.add_argument(
        '--url',
        type=str,
        help='url target environnement',
        required=True)
    import_dictionary_parse.add_argument(
        '--token',
        type=str,
        help='token target environnement',
        required=True)
    import_dictionary_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace target name',
        required=True)
    import_dictionary_parse.add_argument(
        '--version',
        type=str,
        help='version target name')
    import_dictionary_parse.add_argument(
        '--file',
        type=str,
        help='path of the JSON file to import',
        required=True)
    import_dictionary_parse.add_argument(
        '--bulktree',
        action='store_true',
        help='convert a flat JSON file to bulktree before import')


def import_dataprocessings_parse(subparsers):
    # create the parser for the "import_dataprocessings" command
    import_dataprocessings_parse = subparsers.add_parser('import-dataprocessings', help='import-dataprocessings help')
    import_dataprocessings_parse.add_argument(
        '--url',
        type=str,
        help='url target environnement',
        required=True)
    import_dataprocessings_parse.add_argument(
        '--token',
        type=str,
        help='token target environnement',
        required=True)
    import_dataprocessings_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace target name',
        required=True)
    import_dataprocessings_parse.add_argument(
        '--version',
        type=str,
        help='version target name')
    import_dataprocessings_parse.add_argument(
        '--file',
        type=str,
        help='path of the JSON file to import',
        required=True)
    import_dataprocessings_parse.add_argument(
        '--bulktree',
        action='store_true',
        help='convert a flat JSON file to bulktree before import')


def import_usages_parse(subparsers):
    # create the parser for the "import_usages" command
    import_usages_parse = subparsers.add_parser('import-usages', help='import-usages help')
    import_usages_parse.add_argument(
        '--url',
        type=str,
        help='url target environnement',
        required=True)
    import_usages_parse.add_argument(
        '--token',
        type=str,
        help='token target environnement',
        required=True)
    import_usages_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace target name',
        required=True)
    import_usages_parse.add_argument(
        '--version',
        type=str,
        help='version target name')
    import_usages_parse.add_argument(
        '--file',
        type=str,
        help='path of the JSON file to import',
        required=True)
    import_usages_parse.add_argument(
        '--bulktree',
        action='store_true',
        help='convert a flat JSON file to bulktree before import')


def import_links_parse(subparsers):
    # create the parser for the "import_links" command
    import_links_parse = subparsers.add_parser('import-links', help='import-links help')
    import_links_parse.add_argument(
        '--url',
        type=str,
        help='url target environnement',
        required=True)
    import_links_parse.add_argument(
        '--token',
        type=str,
        help='token target environnement',
        required=True)
    import_links_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace target name',
        required=True)
    import_links_parse.add_argument(
        '--version',
        type=str,
        help='version target name')
    import_links_parse.add_argument(
        '--file',
        type=str,
        help='path of the JSON file to import',
        required=True)
