from dateutil.parser import isoparse
import time

from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.util import TimeUtils

from src.pipeline_configuration_spec import *


log = Logger(__name__)


def rqa_time_range_filter(user, messages_traced_data, pipeline_config):
    """
    Filters a list of td for research question messages received within the given time range.

    :param messages_traced_data: List of message objects to filter.
    :type messages_traced_data: list of TracedData
    :pipeline_config: pipeline configuration module
    :type PIPELINE_CONFIGURATION:
    :return: Filtered list.
    :rtype: list of TracedData
    """

    # Inclusive start time of the time range to keep. Messages sent before this time will be dropped.
    start_time_inclusive = pipeline_config.project_start_date

    # Inclusive end time of the time range to keep. Messages sent after this time will be dropped.
    end_time_inclusive = pipeline_config.project_end_date

    if start_time_inclusive is None and end_time_inclusive is None:
        log.info("No time range filters specified, returning input data unchanged")
        return messages_traced_data

    time_range_log = ""
    if start_time_inclusive is not None:
        time_range_log += f", modified on or after {start_time_inclusive.isoformat()}"
    if end_time_inclusive is not None:
        time_range_log += f", modified on or before {end_time_inclusive.isoformat()}"

    log.debug(f"Filtering out research question messages{time_range_log}...")

    # Filter a list of td for research question messages received within the given time range.
    rqa_engagement_db_datasets = []
    for analysis_config in pipeline_config.analysis_config:
        if analysis_config.dataset_type == DatasetTypes.RESEARCH_QUESTION_ANSWER:
            for engagement_db_dataset in analysis_config.engagement_db_datasets:
                rqa_engagement_db_datasets.append(engagement_db_dataset)

    filtered = []
    for td in messages_traced_data:
        if td["dataset"] in rqa_engagement_db_datasets:
            if start_time_inclusive is not None and isoparse(td["timestamp"]) < start_time_inclusive:
                continue
            if end_time_inclusive is not None and isoparse(td["timestamp"]) > end_time_inclusive:
                continue
            td.append_data(td, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))
            filtered.append(td)
        else:
            filtered.append(td)

    log.info(f"Filtered out messages{time_range_log}. "
             f"Returning {len(filtered)}/{len(messages_traced_data)} messages.")

    return filtered


def filter_test_participants(user, participants_traced_data_map, test_contacts):
    """
    Filters a dict of participants who are not in pipeline_config.test_contacts e.g AVF/Aggregator staff

    :param participants_traced_data_map: A dict of TracedData individuals objects to filter.
    :type participants_traced_data_map: dict of TracedData
    :param test_contacts: a list containing test participant uids.
    :type test_contacts: list of str
    :return: Filtered dict.
    :rtype: dict of participant_uid -> TracedData
    """
    log.debug("Filtering out test messages...")
    filtered = {}

    for uid, trace_data in participants_traced_data_map.items():
        if uid in test_contacts:
            continue

        trace_data.append_data(trace_data, Metadata(user, Metadata.get_call_location(), time.time()))
        filtered[uid] = trace_data

    log.info(f"Filtered out test messages. "
             f"Returning {len(filtered)}/{len(participants_traced_data_map)} messages.")
    return filtered


def filter_messages(user, messages_data, pipeline_config):

    # Filter out runs sent outwith the project start and end dates
    messages_data = rqa_time_range_filter(user, messages_data, pipeline_config)

    return messages_data


def filter_participants(user, participants_traced_data_map, pipeline_config):
    # Filter out test messages sent by Test Contacts.
    if pipeline_config.filter_test_messages:
        participants_traced_data_map = filter_test_participants(user, participants_traced_data_map, pipeline_config.test_participant_uids)
    else:
        log.debug(
            "Not filtering out test messages (because the pipeline_config.filter_test_messages was set to false)")

    return participants_traced_data_map
