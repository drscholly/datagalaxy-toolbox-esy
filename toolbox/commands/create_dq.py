import json
import logging
from typing import Optional

from toolbox.api.datagalaxy_api_modules import DataGalaxyApiModules
from toolbox.api.http_client import HttpClient
from toolbox.commands.utils import config_workspace


def create_dq(url: str,
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
            items = json.load(f)
    except Exception as e:
        logging.error(f"Failed to read rules file '{file_path}': {e}")
        return 1

    if isinstance(items, dict):
        items = [items]

    urn_module_api = DataGalaxyApiModules(
        url=url,
        token=token,
        workspace=workspace,
        module="URN",
        http_client=http_client
    )

    dq_module_api = DataGalaxyApiModules(
        url=url,
        token=token,
        workspace=workspace,
        module="DQ",
        http_client=http_client
    )

    for item in items:
        urn = item.get("urn")
        if not urn:
            continue

        entity = urn_module_api.get_by_urn(urn)
        if not entity or 'id' not in entity:
            logging.warning(f"Entity not found for URN: {urn}")
            continue

        # check if rules already exist, if so delete them
        existing_rules = dq_module_api.get_dq_rules(entity['id'])
        for rule in existing_rules:
            dq_module_api.delete_dq_rule(rule['id'])

        rule_data = item.get("rule")
        if not rule_data:
            continue

        rule = {
            "entityId": entity['id'],
            "statement": rule_data.get("statement", ""),
            "code": rule_data.get("code", ""),
            "type": rule_data.get("type", "Consistency")
        }
        logging.info(f"Creating rule {rule} for urn {urn}")
        rule_res = dq_module_api.create_dq_rule(rule)
        rule_id = rule_res.get('ruleId') if isinstance(rule_res, dict) else None

        if not rule_id:
            continue

        checks = item.get("checks", [])
        if "check" in item and item["check"]:
            checks.append(item["check"])

        for check in checks:
            logging.info(f"Creating check {check} for urn {urn}")
            dq_module_api.create_dq_check(rule_id=rule_id, check=check)

    return 0


def create_dq_parse(subparsers):
    # create the parser for the "create-dq" command
    create_dq_parse = subparsers.add_parser('create-dq', help='create-dq help')
    create_dq_parse.add_argument(
        '--url',
        type=str,
        help='url source',
        required=True)
    create_dq_parse.add_argument(
        '--token',
        type=str,
        help='token',
        required=True)
    create_dq_parse.add_argument(
        '--workspace',
        type=str,
        help='workspace name',
        required=False)
    create_dq_parse.add_argument(
        '--file',
        type=str,
        default='dq_rules.json',
        help='path to JSON file containing DQ rules and checks (default: dq_rules.json)',
        required=False)
