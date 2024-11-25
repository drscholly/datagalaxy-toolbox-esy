from toolbox.api.datagalaxy_api_glossary import DataGalaxyApiGlossary
from toolbox.api.datagalaxy_api_workspaces import DataGalaxyApiWorkspace
from toolbox.commands.copy_glossary import copy_glossary
from toolbox.api.datagalaxy_api import DataGalaxyBulkResult
import pytest as pytest


# Mocks

def mock_list_properties_on_source_workspace(self, workspace_name):
    if workspace_name == 'workspace_source':
        return ['property1', 'property1', 'property1']
    return []


# Scenarios

def test_copy_glossary_when_no_property_on_target(mocker):
    # GIVEN
    workspaces = mocker.patch.object(DataGalaxyApiWorkspace, 'list_workspaces', autospec=True)
    workspaces.return_value = ['workspace_source', 'workspace_target']
    workspace_source_mock = mocker.patch.object(DataGalaxyApiWorkspace, 'get_workspace', autospec=True)
    workspace_source_mock.return_value = 'workspace_source'
    glossary_properties_on_source_workspace_mock = mocker.patch.object(
        DataGalaxyApiGlossary,
        'list_properties',
        autospec=True
    )
    glossary_properties_on_source_workspace_mock.side_effect = mock_list_properties_on_source_workspace
    bulk_upsert_properties_on_target_workspace_mock = mocker.patch.object(
        DataGalaxyApiGlossary,
        'bulk_upsert_property_tree',
        autospec=True
    )
    bulk_upsert_properties_on_target_workspace_mock.return_value = DataGalaxyBulkResult(
        total=3,
        created=3,
        updated=0,
        unchanged=0,
        deleted=0
    )

    # THEN
    result = copy_glossary(
        url_source='url_source',
        token_source='token_source',
        url_target='url_target',
        token_target='token_target',
        workspace_source_name='workspace_source',
        workspace_target_name='workspace_target',
        tag_value=None
    )

    # ASSERT / VERIFY

    assert result == DataGalaxyBulkResult(total=3, created=3, updated=0, unchanged=0, deleted=0)
    assert glossary_properties_on_source_workspace_mock.call_count == 1
    assert bulk_upsert_properties_on_target_workspace_mock.call_count == 1


def test_copy_glossary_when_workspace_target_does_not_exist(mocker):
    # GIVEN
    workspaces = mocker.patch.object(DataGalaxyApiWorkspace, 'list_workspaces', autospec=True)
    workspaces.return_value = ['workspace_source']
    workspace_source_mock = mocker.patch.object(DataGalaxyApiWorkspace, 'get_workspace', autospec=True)
    workspace_source_mock.return_value = None
    glossary_properties_on_source_workspace_mock = mocker.patch.object(DataGalaxyApiGlossary,
                                                                       'list_properties',
                                                                       autospec=True)
    glossary_properties_on_source_workspace_mock.side_effect = mock_list_properties_on_source_workspace

    # ASSERT / VERIFY
    with pytest.raises(Exception, match='workspace workspace_target does not exist'):
        copy_glossary(
            url_source='url_source',
            token_source='token_source',
            url_target='url_target',
            token_target='token_target',
            workspace_source_name='workspace_source',
            workspace_target_name='workspace_target',
            tag_value=None
        )
