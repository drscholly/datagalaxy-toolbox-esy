from toolbox.api.datagalaxy_api import get_access_token, Token, DataGalaxyBulkResult
from toolbox.api.datagalaxy_api_glossary import DataGalaxyApiGlossary
from toolbox.api.datagalaxy_api_workspaces import DataGalaxyApiWorkspace
import json


def reverse_tree(objects):
    # Create a dictionary to store nodes by their parent name
    nodes_by_parent = {}

    # Organize nodes by their parent name
    for obj in objects:
        parent_name = obj.get('parent', {}).get('name')  # Get the parent name
        if parent_name not in nodes_by_parent:
            nodes_by_parent[parent_name] = []
        nodes_by_parent[parent_name].append(obj)

    # Function to recursively build the reversed tree
    def build_tree(parent_name, parent_description=None):
        node = {'name': parent_name, 'type': 'BusinessDomain', 'children': []}
        if parent_description is not None:
            node['description'] = parent_description
        if parent_name in nodes_by_parent:
            for child_node in nodes_by_parent[parent_name]:
                child_name = child_node['name']
                child_description = child_node.get('description')
                child = build_tree(child_name, child_description)
                node['children'].append(child)
        return node

    # Build the reversed tree starting from root nodes (parent_name == None)
    reversed_tree = build_tree(None)
    return reversed_tree


def laposte(url: str,
            token: str,
            workspace_name: str) -> DataGalaxyBulkResult:

    integration_token = Token(token)
    access_token = get_access_token(url, integration_token)
    workspaces_api = DataGalaxyApiWorkspace(
        url=url,
        access_token=access_token)

    workspace = workspaces_api.get_workspace(workspace_name)
    if not workspace:
        raise Exception(f'workspace {workspace_name} does not exist')

    # workspace target glossary
    glossary_on_workspace = DataGalaxyApiGlossary(
        url=url,
        access_token=access_token,
        workspace=workspace
    )

    # domains
    file_path = "/home/esy/datagalaxy-toolbox-esy/data/domains.json"
    with open(file_path, 'r') as file:
        data = json.load(file)

    for obj in data['results']:
        if "community" in obj:
            obj["parent"] = obj.pop("community")
    test = data['results']

    # communities
    file_path = "/home/esy/datagalaxy-toolbox-esy/data/communities.json"
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    data['results'].extend(test) 

    reversed_tree = reverse_tree(data['results'])
    reversed_tree['name'] = "Domaine transverse 2"
    reversed_tree['type'] = "BusinessDomainGroup"
    print(reversed_tree)

    # return 0
    return glossary_on_workspace.bulk_upsert_property_tree(
        workspace_name=workspace_name,
        properties=reversed_tree
    )


def laposte_parse(subparsers):
    # create the parser for the "laposte" command
    laposte_parse = subparsers.add_parser('laposte', help='laposte help')
    laposte_parse.add_argument(
        '--url',
        type=str,
        help='url environnement',
        required=True)
    laposte_parse.add_argument(
        '--token',
        type=str,
        help='integration token environnement',
        required=True)
    laposte_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace name',
        required=True)
