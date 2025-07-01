import logging
import requests as requests


class DataGalaxyApiNewModules:
    def __init__(self, url: str, token: str, workspace: dict, module: str):
        if module not in ["Strategy", "Governance", "Products"]:
            raise Exception('The specified new module does not exist.')
        self.module = module
        if module == "Strategy":
            self.types = ["Objective", "Initiative", "UseCase"]
        if module == "Governance":
            self.types = ["PolicyGroup", "Policy", "RuleGroup", "Rule", "MonitorGroup", "Monitor"]
        if module == "Products":
            self.types = ["DataProduct", "AiProduct"]

        self.url = url
        self.token = token
        self.workspace = workspace

    def list_objects(self, workspace_name: str, include_links=False) -> list:
        objects = []
        version_id = self.workspace['versionId']
        headers = {'Authorization': f"Bearer {self.token}"}
        for type in self.types:
            params = {'versionId': version_id, 'limit': '5000', 'type': type}
            if include_links is True:
                params['includeLinks'] = 'true'
            else:
                params['includeAttributes'] = 'true'
            response = requests.get(f"{self.url}/objects", params=params, headers=headers)
            code = response.status_code
            body_json = response.json()
            if code != 200:
                raise Exception(body_json['error'])
            logging.info(
                f'list_objects - {len(body_json["results"])} objects found on '
                f'workspace {workspace_name} in module {self.module} of type {type}')
            result_pages = [body_json['results']]
            next_page = body_json["next_page"]
            while next_page is not None:
                logging.info('Fetching another page from the API...')
                headers = {'Authorization': f"Bearer {self.token}"}
                response = requests.get(next_page, headers=headers)
                body_json = response.json()
                logging.info(
                    f'list_objects - {len(body_json["results"])} objects found on '
                    f'workspace {workspace_name} in module {self.module} of type {type}')
                next_page = body_json["next_page"]
                result_pages.append(body_json['results'])
            objects.append(result_pages)
        return objects
