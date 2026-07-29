import json
from unittest.mock import MagicMock
from toolbox.commands.create_urn import create_urn


def test_create_urn_from_file(tmp_path, mocker):
    urn_data = {
        "objects": [
            {
                "urn": "urn:test:entity"
            }
        ],
        "links": []
    }

    file_path = tmp_path / "urn_objects.json"
    file_path.write_text(json.dumps(urn_data), encoding="utf-8")

    mock_config_workspace = mocker.patch("toolbox.commands.create_urn.config_workspace", return_value="ws-id")
    mock_api_class = mocker.patch("toolbox.commands.create_urn.DataGalaxyApiModules")

    module_api_mock = MagicMock()
    mock_api_class.return_value = module_api_mock

    http_client = MagicMock()
    res = create_urn("https://api.test", "token123", "ws-name", str(file_path), http_client)

    assert res == 0
    mock_config_workspace.assert_called_once()
    mock_api_class.assert_called_once_with(
        url="https://api.test",
        token="token123",
        workspace="ws-id",
        module="URN",
        http_client=http_client
    )
    module_api_mock.create_urn.assert_called_once_with(urn_data)
