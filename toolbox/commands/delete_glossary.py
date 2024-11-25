from toolbox.api.datagalaxy_api import DataGalaxyBulkResult
from toolbox.api.datagalaxy_api_glossary import DataGalaxyApiGlossary
from toolbox.api.datagalaxy_api_workspaces import DataGalaxyApiWorkspace
import logging


def delete_glossary(url: str,
                    token: str,
                    workspace_name: str) -> DataGalaxyBulkResult:

    workspaces_api = DataGalaxyApiWorkspace(
        url=url,
        token=token)

    workspace = workspaces_api.get_workspace(workspace_name)

    if not workspace:
        raise Exception(f'workspace {workspace_name} does not exist')

    # on récupère les propriétés du glossary du workspace_source
    glossary_api = DataGalaxyApiGlossary(
        url=url,
        token=token,
        workspace=workspace
    )
    glossary_properties = glossary_api.list_properties(
        workspace_name)

    ids = list(map(lambda object: object['id'], glossary_properties))

    if ids is None or len(ids) < 1:
        logging.warn("Nothing to delete in this module")
        return 0

    return glossary_api.delete_objects(
        workspace_name=workspace_name,
        ids=ids
    )


def delete_glossary_parse(subparsers):
    # create the parser for the "delete_glossary" command
    delete_glossary_parse = subparsers.add_parser('delete-glossary', help='delete-glossary help')
    delete_glossary_parse.add_argument(
        '--url',
        type=str,
        help='url environnement',
        required=True)
    delete_glossary_parse.add_argument(
        '--token',
        type=str,
        help='token',
        required=True)
    delete_glossary_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace name',
        required=True)
