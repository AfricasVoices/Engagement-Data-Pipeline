import requests
import json
from dateutil.parser import isoparse

from storage.google_cloud import google_cloud_utils
from core_data_modules.logging import Logger

log = Logger(__name__)

BASE_URL = "https://eu.kobotoolbox.org/api/v2/assets"
PAGE_SIZE = 1000  # Maximum allowed per page as of January 2026


class KoboToolBoxClient:
    def get_authorization_headers(google_cloud_credentials_file_path, token_file_url):
        """
        Retrieves a KoboToolBox API token and returns it as a dictionary of authorization headers.

        :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use when
                                                downloading api token.
        :type google_cloud_credentials_file_path: str
        :param token_file_url: Path to the Google Cloud file path that contains KoboToolBox account api token.
        :type token_file_url: str
        :return: A dictionary of authorization headers containing the KoboToolBox API token.
        :rtype: dict
        """
        log.info('Downloading KoboToolBox access token...')
        api_token = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, token_file_url).strip())

        authorization_headers = {"Authorization": f'Token {api_token["api_token"]}'}

        return authorization_headers


    def get_form_responses(authorization_headers, asset_uid, submitted_after_exclusive=None):
        """
        Retrieves all responses for a specified KoboToolBox form, handling pagination automatically.

        Iterates through all pages of results by following the `next` link returned in each API
        response, using a page size of up to 1,000 records (the maximum allowed as of January 2026).

        :param authorization_headers: A dictionary of authorization headers for the API call 
        :type authorization_headers: dict
        :param asset_uid: The UID of the form for which responses are to be retrieved.
        :type asset_uid: str
        :param submitted_after_exclusive: A datetime object specifying the earliest submission time. If set, only downloads responses last
                                        submitted after this datetime. If None, downloads responses from all of time.
        :type submitted_after_exclusive: datetime.datetime | None
        :raises: requests.exceptions.RequestException: If an error occurs while making the API call.
        :return: A list of dictionaries, each representing a response to the specified form.
        :rtype: list of dict
        
        Examples:
            To retrieve all responses for a kobotoolbox form:

            >>> authorization_headers = {'Authorization': 'Bearer your_token'}
            >>> asset_uid = 'your_form_uid'
            >>> form_responses = get_form_responses(authorization_headers, asset_uid)
            >>> print(len(form_responses))
            100

            To retrieve responses submitted after a specific time:

            >>> authorization_headers = {'Authorization': 'Bearer your_token'}
            >>> asset_uid = 'your_form_uid'
            >>> submitted_after_exclusive = datetime.datetime(2022, 1, 1)
            >>> form_responses = get_form_responses(authorization_headers, asset_uid, submitted_after_exclusive)
            >>> print(len(form_responses))
            50
        """
        timestamp_log = ""
        query_param = ""

        if submitted_after_exclusive is not None:
            submitted_after_exclusive = submitted_after_exclusive.isoformat()
            timestamp_log = f", last submitted after {submitted_after_exclusive}"
            query_param = f'&query={{"_submission_time":{{"$gt":"{submitted_after_exclusive}"}}}}'

        log.info(f"Downloading responses for Asset '{asset_uid}'{timestamp_log}")

        # Build the initial paginated request
        next_url = f"{BASE_URL}/{asset_uid}/data/?format=json&limit={PAGE_SIZE}&start=0{query_param}"

        all_responses = []
        page_num = 1

        while next_url:
            log.info(f"Fetching page {page_num} from: {next_url}")
            response = requests.get(next_url, headers=authorization_headers, verify=False)

            if not response.content:
                log.warning(
                    f"Empty response on page {page_num} for Asset '{asset_uid}'{timestamp_log}. "
                    f"Status code: {response.status_code}"
                )
                break

            response.raise_for_status()
            response_data = json.loads(response.content)
            page_results = response_data.get("results", [])
            all_responses.extend(page_results)

            log.info(f"Page {page_num}: fetched {len(page_results)} responses "
                     f"(running total: {len(all_responses)})")

            # Follow the `next` link if present, otherwise stop
            next_url = response_data.get("next")
            page_num += 1

        log.info(f"Downloaded {len(all_responses)} total responses for Asset '{asset_uid}'{timestamp_log}")
        return all_responses
