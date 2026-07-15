import json

from toolbox.api.datagalaxy_api_modules import DataGalaxyApiModules
from toolbox.api.http_client import HttpClient
from toolbox.commands.import_module import import_module


def test_import_module_reads_flat_file_and_imports_built_bulktree(mocker, tmp_path):
    # GIVEN
    input_file = tmp_path / "glossary.json"
    input_file.write_text(
        json.dumps([
            {
                "path": "\\domain",
                "functionalPath": "\\Domain",
                "typePath": "\\Glossary",
                "attributes": {"summary": "Root summary"},
            },
            {
                "path": "\\domain\\term",
                "functionalPath": "\\Domain\\Term",
                "typePath": "\\Glossary\\Term",
                "attributes": {
                    "description": "Term description",
                    "technologyCode": "python",
                },
            },
        ]),
        encoding="utf-8",
    )
    mocker.patch(
        "toolbox.commands.import_module.config_workspace",
        return_value={"name": "workspace", "versionId": "version_id"},
    )
    bulk_upsert_tree_mock = mocker.patch.object(DataGalaxyApiModules, "bulk_upsert_tree", autospec=True)
    bulk_upsert_tree_mock.return_value = 200

    # THEN
    result = import_module(
        module="Glossary",
        url="url",
        token="token",
        workspace_name="workspace",
        version_name=None,
        file_path=str(input_file),
        http_client=HttpClient(verify_ssl=True),
        bulktree=True,
    )

    # ASSERT / VERIFY
    assert result == 0
    assert bulk_upsert_tree_mock.call_count == 1
    assert bulk_upsert_tree_mock.call_args.kwargs["workspace_name"] == "workspace"
    assert bulk_upsert_tree_mock.call_args.kwargs["tag_value"] is None
    assert bulk_upsert_tree_mock.call_args.kwargs["bulktree"] == [
        {
            "name": "Domain",
            "technicalName": "domain",
            "type": "Glossary",
            "summary": "Root summary",
            "children": [
                {
                    "name": "Term",
                    "technicalName": "term",
                    "type": "Term",
                    "description": "Term description",
                    "children": [],
                }
            ],
        }
    ]
