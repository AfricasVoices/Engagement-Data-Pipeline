from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

import pytz


class FlowResultConfiguration:
    def __init__(self, flow_name, flow_result_field, engagement_db_dataset,
                 created_after_inclusive=pytz.timezone("utc").localize(datetime.min),
                 created_before_exclusive=pytz.timezone("utc").localize(datetime.max)):
        """
        Configuration for syncing one flow result field to an engagement database.

        :param flow_name: Name of flow to fetch the results from.
        :type flow_name: str
        :param flow_result_field: Name of the result field in the flow.
        :type flow_result_field: str
        :param engagement_db_dataset: Name of the dataset to use in the engagement database.
        :type engagement_db_dataset: str
        :param created_after_inclusive: Start of time-range to filter flow results within. Results created before
                                        this timestamp will not be added to the engagement database.
        :type created_after_inclusive: datetime.datetime
        :param created_before_exclusive: End of time-range to filter flow results within. Results created on or after
                                         this timestamp will not be added to the engagement database.
        :type created_before_exclusive: datetime.datetime
        """
        self.flow_name = flow_name
        self.flow_result_field = flow_result_field
        self.engagement_db_dataset = engagement_db_dataset
        self.created_after_inclusive = created_after_inclusive
        self.created_before_exclusive = created_before_exclusive

    def to_dict(self) -> Dict[str, str]:
        return {
            "flow_name": self.flow_name,
            "flow_result_field": self.flow_result_field,
            "engagement_db_dataset": self.engagement_db_dataset
        }

    @classmethod
    def from_dict(cls, d: Dict[str, str]) -> FlowResultConfiguration:
        flow_name = d["flow_name"]
        flow_result_field = d["flow_result_field"]
        engagement_db_dataset = d["engagement_db_dataset"]

        return cls(flow_name, flow_result_field, engagement_db_dataset)


class UuidFilter:
    """
    A filter for UUIDs used to ensure that messages are processed only if the sender's UUID is valid and
    exists in the specified UUID table.

    param uuid_file_url: The URL to the file containing the list of valid UUIDs.
    :type uuid_file_url: str
    """

    def __init__(self, uuid_file_url: str):
        """
        Initializes the UuidFilter instance with the URL or path to the UUID file.

        :param uuid_file_url: The URL or path to the file containing valid UUIDs.
        :type uuid_file_url: str
        """
        self.uuid_file_url = uuid_file_url


class RapidProToEngagementDBConfiguration:
    """
    Configuration for syncing flow results to an engagement database, with optional UUID filtering.

    :param flow_result_configurations: A list of `FlowResultConfiguration` objects specifying the flow result fields
                                       and the corresponding engagement database datasets.
    :type flow_result_configurations: list of FlowResultConfiguration
    :param uuid_filter: Optional filter to ensure messages are processed only if the sender's UUID is valid and
                        exists in the specified UUID table.
    :type uuid_filter: UuidFilter, optional
    """

    def __init__(self, flow_result_configurations: [FlowResultConfiguration], uuid_filter: Optional[UuidFilter] = None):
        """
        Initializes the configuration for syncing flow results to an engagement database.

        :param flow_result_configurations: A list of `FlowResultConfiguration` objects specifying the flow result fields
                                           and the corresponding engagement database datasets.
        :type flow_result_configurations: list of FlowResultConfiguration
        :param uuid_filter: Optional filter to ensure messages are processed only if the sender's UUID is valid and
                            exists in the specified UUID table.
        :type uuid_filter: UuidFilter, optional
        """
        self.flow_result_configurations = flow_result_configurations
        self.uuid_filter = uuid_filter
