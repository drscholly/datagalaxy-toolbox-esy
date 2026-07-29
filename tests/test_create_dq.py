import json
from unittest.mock import MagicMock
from toolbox.commands.create_dq import create_dq


def test_create_dq_from_file(tmp_path, mocker):
    rules_data = [
        {
            "urn": "urn:test:entity",
            "rule": {
                "statement": "col1 IS NOT NULL",
                "code": "null_constraint",
                "type": "Consistency"
            },
            "check": {
                "status": "Passed",
                "message": "All good",
                "detail": "No nulls"
            }
        }
    ]

    rules_file = tmp_path / "rules.json"
    rules_file.write_text(json.dumps(rules_data), encoding="utf-8")

    mock_config_workspace = mocker.patch("toolbox.commands.create_dq.config_workspace", return_value="ws-id")
    mock_api_class = mocker.patch("toolbox.commands.create_dq.DataGalaxyApiModules")

    urn_api_mock = MagicMock()
    dq_api_mock = MagicMock()

    # DataGalaxyApiModules instantiated twice: first URN, second DQ
    mock_api_class.side_effect = [urn_api_mock, dq_api_mock]

    urn_api_mock.get_by_urn.return_value = {"id": "entity-123"}
    dq_api_mock.get_dq_rules.return_value = [{"id": "old-rule-1"}]
    dq_api_mock.create_dq_rule.return_value = {"ruleId": "new-rule-456"}

    http_client = MagicMock()
    res = create_dq("https://api.test", "token123", "ws-name", str(rules_file), http_client)

    assert res == 0
    mock_config_workspace.assert_called_once()
    urn_api_mock.get_by_urn.assert_called_once_with("urn:test:entity")
    dq_api_mock.get_dq_rules.assert_called_once_with("entity-123")
    dq_api_mock.delete_dq_rule.assert_called_once_with("old-rule-1")
    dq_api_mock.create_dq_rule.assert_called_once_with({
        "entityId": "entity-123",
        "statement": "col1 IS NOT NULL",
        "code": "null_constraint",
        "type": "Consistency"
    })
    dq_api_mock.create_dq_check.assert_called_once_with(
        rule_id="new-rule-456",
        check={"status": "Passed", "message": "All good", "detail": "No nulls"}
    )
