import logging

from toolbox.api.datagalaxy_api import get_access_token, Token
from toolbox.api.datagalaxy_api_attributes import DataGalaxyApiAttributes


def copy_tags(url_source: str,
              url_target: str,
              token_source_value: str,
              token_target_value: str) -> int:
    token_source = Token(token_source_value)
    token_target = Token(token_target_value)

    # source_client_space_id = token_source.get_client_space_id()
    # target_client_space_id = token_target.get_client_space_id()

    access_token_source = get_access_token(url_source, token_source)
    access_token_target = get_access_token(url_target, token_target)

    attributes_api_source = DataGalaxyApiAttributes(url=url_source, access_token=access_token_source)
    attributes_api_target = DataGalaxyApiAttributes(url=url_target, access_token=access_token_target)

    source_tags = attributes_api_source.list_values("Common", "Domains")
    target_tags = attributes_api_target.list_values("Common", "Domains")

    test = merge_tags_lists(source_tags, target_tags, "label")

    return 0


def merge_tags_lists(source_list, target_list, label_property):
    # Create a set of labels from the target list for faster lookup
    target_labels = {obj[label_property] for obj in target_list}

    # Iterate over each object in the source list
    for obj in source_list:
        # Check if the label of the current object exists in the target list
        if obj[label_property] not in target_labels:
            logging.info(f'{obj[label_property]} not found in target labels.')
            # If the label doesn't exist, add the object to the target list
            target_list.append(obj)
            # print("Added object to target list:", obj)
            # Update the set of labels in the target list
            target_labels.add(obj[label_property])
        else:
            logging.info(f'{obj[label_property]} already exists in target labels. Skipping.')

    # Return the updated target list
    logging.info(f'\nFinal target list: {target_list}')
    return target_list

def copy_tags_parse(subparsers):
    # create the parser for the "copy-tags" command
    copy_tags_parse = subparsers.add_parser('copy-tags', help='copy-tags help')
    copy_tags_parse.add_argument(
        '--url-source',
        type=str,
        help='url source',
        required=True)
    copy_tags_parse.add_argument(
        '--url-target',
        type=str,
        help='url target',
        required=True)
    copy_tags_parse.add_argument(
        '--token-source',
        type=str,
        help='integration source token',
        required=True)
    copy_tags_parse.add_argument(
        '--token-target',
        type=str,
        help='integration target token',
        required=True)
