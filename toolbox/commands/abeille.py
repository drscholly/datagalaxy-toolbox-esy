from toolbox.api.datagalaxy_api import get_access_token, Token, DataGalaxyBulkResult
from toolbox.api.datagalaxy_api_dataprocessings import DataGalaxyApiDataprocessings
from toolbox.api.datagalaxy_api_workspaces import DataGalaxyApiWorkspace
import json
import logging


def abeille(url: str,
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

    # workspace target dps
    dataprocessings_on_workspace = DataGalaxyApiDataprocessings(
        url=url,
        access_token=access_token,
        workspace=workspace
    )

    bulk_tree = {}

    # datastage
    file_path = "/home/esy/datagalaxy-toolbox-esy/data/datastage.json"
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Multiple errors checks
    if not isinstance(data, dict):
        logging.error("Unexpected data type")

    if "DSExport" not in data:
        logging.error("DSExport not present")
    data = data["DSExport"]

    if "Job" not in data:
        logging.error("Job not DSExport")
    job = data["Job"]

    # identifier
    if "@Identifier" not in job:
        logging.error("Identifier not in Job")
    print(job["@Identifier"])
    bulk_tree['name'] = job["@Identifier"]
    bulk_tree['type'] = "DataFlow"
    bulk_tree['children'] = []

    # job records
    if "Record" not in job:
        logging.error("Record not in Job")
    job_records = job["Record"]

    for record in job_records:
        if "@Type" not in record:
            logging.error("Type not in Record")
        # JobDefn : job type
        if record["@Type"] == "JobDefn":
            props = record["Property"]
            for prop in props:
                if prop["@Name"] == "JobType":
                    print("Job type : ", prop["#text"])
                    bulk_tree['description'] = "Job type : " + prop["#text"]

        # CustomStage : stage name
        if record["@Type"] == "CustomStage":
            props = record["Property"]
            for prop in props:
                if prop["@Name"] == "Name":
                    print("stage name : ", prop["#text"])
                    bulk_tree['children'].append({'name': prop["#text"], 'type': "DataProcessing"})

    print(bulk_tree)
    return dataprocessings_on_workspace.bulk_upsert_dataprocessings_tree(
        workspace_name=workspace_name,
        dataprocessings=[bulk_tree],
        tag_value=None
    )


def abeille_parse(subparsers):
    # create the parser for the "abeille" command
    abeille_parse = subparsers.add_parser('abeille', help='abeille help')
    abeille_parse.add_argument(
        '--url',
        type=str,
        help='url environnement',
        required=True)
    abeille_parse.add_argument(
        '--token',
        type=str,
        help='integration token environnement',
        required=True)
    abeille_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace name',
        required=True)
