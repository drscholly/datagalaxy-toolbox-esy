from typing import Optional
import logging

from toolbox.api.datagalaxy_api_new_modules import DataGalaxyApiNewModules
from toolbox.commands.utils import config_workspace


def copy_new_module(module: str,
                    url_source: str,
                    url_target: Optional[str],
                    token_source: str,
                    token_target: Optional[str],
                    workspace_source_name: str,
                    version_source_name: Optional[str],
                    workspace_target_name: str,
                    version_target_name: Optional[str],
                    tag_value: Optional[str]) -> int:
    # Tokens
    if token_target is None:
        token_target = token_source

    # URL
    if url_target is None:
        url_target = url_source

    # Source workspace
    source_workspace = config_workspace(
        mode="source",
        url=url_source,
        token=token_source,
        workspace_name=workspace_source_name,
        version_name=version_source_name
    )
    if not source_workspace:
        return 1

    # Target workspace
    target_workspace = config_workspace(
        mode="target",
        url=url_target,
        token=token_target,
        workspace_name=workspace_target_name,
        version_name=version_target_name
    )
    if not target_workspace:
        return 1

    # Source module
    source_module_api = DataGalaxyApiNewModules(
        url=url_source,
        token=token_source,
        workspace=source_workspace,
        module=module
    )

    # Target module
    target_module_api = DataGalaxyApiNewModules(
        url=url_target,
        token=token_target,
        workspace=target_workspace,
        module=module
    )

    # Fetch objects from source workspace
    source_objects = source_module_api.list_objects(workspace_source_name)
    test_obj = {}
    for type_pages in source_objects:
        for page in type_pages:
            for obj in page:
                logging.info(obj['path'])
                test_obj = obj

    target_module_api.create_object(
        workspace_name=target_workspace,
        new_object=test_obj,
        parent_id=None
    )

    # if source_objects == [[]]:
    #     logging.warning(f'copy-module - No object in source workspace {workspace_source_name}, aborting.')
    #     return 1

    # Create objects on target workspace
    # target_module_api.bulk_upsert_tree(
    #     workspace_name=workspace_target_name,
    #     objects=source_objects,
    #     tag_value=tag_value
    # )

    return 0


# Parsers
def copy_strategy_parse(subparsers):
    # create the parser for the "copy_strategy" command
    copy_strategy_parse = subparsers.add_parser('copy-strategy', help='copy-strategy help')
    copy_strategy_parse.add_argument(
        '--url-source',
        type=str,
        help='url source environnement',
        required=True)
    copy_strategy_parse.add_argument(
        '--token-source',
        type=str,
        help='token source environnement',
        required=True)
    copy_strategy_parse.add_argument(
        '--url-target',
        type=str,
        help='url target environnement (if undefined, use url source)')
    copy_strategy_parse.add_argument(
        '--token-target',
        type=str,
        help='token target environnement (if undefined, use token source)')
    copy_strategy_parse.add_argument(
        '--workspace-source',
        type=str,
        help='workspace source name',
        required=True)
    copy_strategy_parse.add_argument(
        '--version-source',
        type=str,
        help='version source name')
    copy_strategy_parse.add_argument(
        '--workspace-target',
        type=str,
        help='workspace target name',
        required=True)
    copy_strategy_parse.add_argument(
        '--version-target',
        type=str,
        help='version target name')
    copy_strategy_parse.add_argument(
        '--tag-value',
        type=str,
        help='select tag value to filter objects')


def copy_governance_parse(subparsers):
    # create the parser for the "copy_governance" command
    copy_governance_parse = subparsers.add_parser('copy-governance', help='copy-governance help')
    copy_governance_parse.add_argument(
        '--url-source',
        type=str,
        help='url source environnement',
        required=True)
    copy_governance_parse.add_argument(
        '--token-source',
        type=str,
        help='token source environnement',
        required=True)
    copy_governance_parse.add_argument(
        '--url-target',
        type=str,
        help='url target environnement (if undefined, use url source)')
    copy_governance_parse.add_argument(
        '--token-target',
        type=str,
        help='token target environnement (if undefined, use token source)')
    copy_governance_parse.add_argument(
        '--workspace-source',
        type=str,
        help='workspace source name',
        required=True)
    copy_governance_parse.add_argument(
        '--version-source',
        type=str,
        help='version source name')
    copy_governance_parse.add_argument(
        '--workspace-target',
        type=str,
        help='workspace target name',
        required=True)
    copy_governance_parse.add_argument(
        '--version-target',
        type=str,
        help='version target name')
    copy_governance_parse.add_argument(
        '--tag-value',
        type=str,
        help='select tag value to filter objects')


def copy_products_parse(subparsers):
    # create the parser for the "copy_products" command
    copy_products_parse = subparsers.add_parser('copy-products', help='copy-products help')
    copy_products_parse.add_argument(
        '--url-source',
        type=str,
        help='url source environnement',
        required=True)
    copy_products_parse.add_argument(
        '--token-source',
        type=str,
        help='token source environnement',
        required=True)
    copy_products_parse.add_argument(
        '--url-target',
        type=str,
        help='url target environnement (if undefined, use url source)')
    copy_products_parse.add_argument(
        '--token-target',
        type=str,
        help='token target environnement (if undefined, use token source)')
    copy_products_parse.add_argument(
        '--workspace-source',
        type=str,
        help='workspace source name',
        required=True)
    copy_products_parse.add_argument(
        '--version-source',
        type=str,
        help='version source name')
    copy_products_parse.add_argument(
        '--workspace-target',
        type=str,
        help='workspace target name',
        required=True),
    copy_products_parse.add_argument(
        '--version-target',
        type=str,
        help='version target name')
    copy_products_parse.add_argument(
        '--tag-value',
        type=str,
        help='select tag value to filter objects')
