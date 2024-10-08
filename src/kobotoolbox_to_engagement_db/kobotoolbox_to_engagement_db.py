import requests
import pandas as pd
import json
from dateutil.parser import isoparse
from collections import OrderedDict

from storage.google_cloud import google_cloud_utils

from core_data_modules.cleaners import PhoneCleaner
from core_data_modules.logging import Logger

from engagement_database.data_models import (Message, MessageDirections, MessageStatuses, MessageOrigin,
                                             HistoryEntryOrigin)

from src.common.cache import Cache
from src.kobotoolbox_to_engagement_db.configuration import KoboToolBoxParticipantIdTypes
from src.kobotoolbox_to_engagement_db.kobotoolbox_client import KoboToolBoxClient
from src.kobotoolbox_to_engagement_db.sync_stats import KoboToolBoxSyncEvents, KoboToolBoxToEngagementDBSyncStats

log = Logger(__name__)

#TODO: Move to src/common
def _validate_phone_number_and_format_as_urn(phone_number, country_code, valid_length, valid_prefixes=None):
    """
    Validates a phone number and formats it as a URN.

    :param phone_number: Phone number to validate and format. This may be just the phone number or the phone number
                         and country code, and may contain punctuation or alpha characters e.g. tel:+ or (0123) 70-40
    :type phone_number: str
    :param country_code: Expected country code. This method ensures the phone number begins with this country code,
                         or adds it if not.
    :type country_code: str
    :param valid_length: Valid length of the phone number, including the country code.
                         This function will fail with a value error if it sees a phone number that doesn't have
                         this length.
    :type valid_length: int
    :param valid_prefixes: Optional list of prefixes to check. If provided, this function will ensure every phone
                           number starts with one of these prefixes. For example, this could be used to ensure
                           this is a mobile number, or to ensure it belongs to a valid network.
    :type valid_prefixes: set of str | None
    :raises ValueError: If the phone number is empty, does not have a valid prefix (when valid_prefixes is provided),
                        or has an invalid length.
    :return: Phone number as urn e.g. 'tel:+254700123123' or None.
    :rtype: str | None
    """
    # Normalise the phone number (removes spaces, non-numeric, and leading 0s).
    phone_number = PhoneCleaner.normalise_phone(phone_number).lstrip('0')

    if not phone_number:
        raise ValueError("Invalid phone number")

   # Validate prefixes and ensure phone number startswidth country code.
    if not phone_number.startswith(country_code):
        if valid_prefixes is not None:
            if not any(phone_number.startswith(p) for p in valid_prefixes):
                raise ValueError(f"Phone number must contain a valid prefix; Valid prefixes specified: {', '.join(valid_prefixes)}")
        phone_number = f"{country_code}{phone_number}"

    # Ensure phone number is the expected length
    if len(phone_number) != valid_length:
        raise ValueError(f"Invalid phone number length; expected length is {valid_length}")

    urn = f"tel:+{phone_number}"

    return urn


def _get_participant_uuid_for_response(response, id_type, participant_id_question_id, uuid_table, form_config):
    """
    Gets the participant_uuid for the given response.

    If the response contains an answer to a question with id `participant_id_question_id`, validates the contact
    info given on the form and formats it as a URN.

    If no answer or question_id is provided or an invalid answer is provided, uses the response id as the participant_uuid 
    instead. In this case, the response id is not de-identified via the uuid table.

    :param response: Response to get the participant uuid for.
    :type response: dict
    :param id_type: A KoboToolBoxParticipantIdTypes
    :type id_type: str
    :param participant_id_question_id: Id of the participant_id question.
    :type participant_id_question_id: str | None
    :param uuid_table: UUID table to use to de-identify the urn
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param form_config: Configuration for the form to sync.
    :type form_config: src.kobotoolbox_to_engagement_db.configuration.KoboToolBoxToEngagementDBConfiguration
    :raises AssertionError: If the id_type is not recognised.
    :raises ValueError: If an invalid participant id is provided and the ignore_invalid_mobile_numbers flag is False.
    :return: Participant uuid for this response.
    :rtype: str
    """
    participant_id_answer = response.get(participant_id_question_id, None)
    response_uuid = f"{response['_id']}_{response['formhub/uuid']}"

    if participant_id_answer is None:
        participant_uuid = response_uuid
    else:
        participant_id = participant_id_answer

        assert id_type == KoboToolBoxParticipantIdTypes.KENYA_MOBILE_NUMBER, \
            f"Participant id type {id_type} not recognised."

        try:
            participant_urn = _validate_phone_number_and_format_as_urn(
                phone_number=participant_id, country_code="254", valid_length=12, valid_prefixes={"10", "11", "7"}
            )
            participant_uuid = uuid_table.data_to_uuid(participant_urn)
        except ValueError as e:
            if form_config.ignore_invalid_mobile_numbers:
                log.warning(f"{e}, using the response_uuid as the participant_uuid instead")
                participant_uuid = response_uuid
            else:
                raise ValueError(f"Invalid participant id: {participant_id}.") from e

    return participant_uuid


def _form_answer_to_engagement_db_message(form_answer, asset_uid, form_response, participant_uuid,
                                          engagement_db_dataset, data_column_name):
    """
    Converts a Form answer to an engagement database message.

    :param form_answer: A string of the response.
    :type form_answer: str
    :param asset_uid: Id of the form this answer is for.
    :type asset_uid: str
    :param form_response: The form response that this answer was given as part of, in KoboToolBox Forms' response dictionary
                          format
    :type form_response: dict
    :param engagement_db_dataset: engagement db dataset name to use for that question.
    :type engagement_db_dataset: str
    :param engagement_db_dataset: engagement db dataset name to use for that question.
    :type engagement_db_dataset: str
    :return: `form_answer` as an engagement db message.
    :rtype: engagement_database.data_models.Message
    """
    return Message(
        participant_uuid=participant_uuid,
        text=form_answer,
        timestamp=isoparse(form_response["_submission_time"]),
        direction=MessageDirections.IN,
        channel_operator="kobotoolbox",  # TODO: Move kobotoolbox to core_data_modules.Codes
        status=MessageStatuses.LIVE,
        dataset=engagement_db_dataset,
        labels=[],
        origin=MessageOrigin(
            origin_id=f"kobotoolbox_form_asset_id_{asset_uid}.response_uuid_{form_response['_uuid']}.data_column_name_{data_column_name}",
            origin_type="kobotoolbox"
        )
    )


def _engagement_db_has_message(engagement_db, message):
    """
    Checks if an engagement database contains a message with the same origin id as the given message.

    :param engagement_db: Engagement database to check for the message.
    :type engagement_db: engagement_database.EngagementDatabase
    :param message: Message to check for existence.
    :type message: engagement_database.data_models.Message
    :return: Whether a message with this text, timestamp, and participant_uuid exists in the engagement database.
    :rtype: bool
    """
    matching_messages_filter = lambda q: q.where("origin.origin_id", "==", message.origin.origin_id)
    matching_messages = engagement_db.get_messages(firestore_query_filter=matching_messages_filter)

    assert len(matching_messages) < 2

    return len(matching_messages) > 0


def _ensure_engagement_db_has_message(engagement_db, message, message_origin_details):
    """
    Ensures that the given message exists in an engagement database.

    This function will only write to the database if a message with the same origin_id doesn't already exist in the
    database.

    :param engagement_db: Engagement database to use.
    :type engagement_db: engagement_database.EngagementDatabase
    :param message: Message to make sure exists in the engagement database.
    :type message: engagement_database.data_models.Message
    :param message_origin_details: Message origin details, to be logged in the HistoryEntryOrigin.details.
    :type message_origin_details: dict
    :return: Sync event.
    :rtype: str
    """
    if _engagement_db_has_message(engagement_db, message):
        log.debug(f"Message already in engagement database")
        return KoboToolBoxSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB

    log.debug(f"Adding message to engagement database dataset {message.dataset}...")
    engagement_db.set_message(
        message,
        HistoryEntryOrigin(origin_name="KoboToolBox -> Database Sync", details=message_origin_details)
    )
    return KoboToolBoxSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB


def _sync_kobotoolbox_to_engagement_db(google_cloud_credentials_file_path, kobotoolbox_source, engagement_db,
                                              uuid_table, cache_path=None):
    """
    Syncs KoboToolBox Forms to an engagement database.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               download KoboToolBox Form credentials.
    :type google_cloud_credentials_file_path: str
    :param kobotoolbox_source: Configuration for the KoboToolBox Forms to sync.
    :type kobotoolbox_source: list of src.koboltoolbox_to_engagement_db.configuration.KoboToolBoxSource
    :param engagement_db: Engagement database to sync
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str | None
    :return: The sync statistics object.
    :rtype: src.kobotoolbox_to_engagement_db.sync_stats.KoboToolBoxToEngagementDBSyncStats
    """
    cache = None
    if cache_path is not None:
        cache = Cache(cache_path)
    last_seen_response_time = None if cache is None else cache.get_date_time(kobotoolbox_source.sync_config.asset_uid)

    authorization_headers = KoboToolBoxClient.get_authorization_headers(google_cloud_credentials_file_path, kobotoolbox_source.token_file_url)
    form_responses = sorted(KoboToolBoxClient.get_form_responses(authorization_headers, kobotoolbox_source.sync_config.asset_uid, last_seen_response_time), 
                            key=lambda response: response['_submission_time'])
    log.info(f"Downloaded {len(form_responses)} responses")

    sync_stats = KoboToolBoxToEngagementDBSyncStats()

    # Check if form_responses is empty and return sync_stats without performing sync operations
    if not form_responses:
        return sync_stats

    for i, form_response in enumerate(form_responses):
        log.info(f"Processing response {i + 1}/{len(form_responses)}...")
        sync_stats.add_event(KoboToolBoxSyncEvents.READ_RESPONSE_FROM_KOBOTOOLBOX_FORM)
        for question_config in kobotoolbox_source.sync_config.question_configurations:

            form_answer = form_response.get(question_config.data_column_name)
            if form_answer is None:
                log.warning(f"Found no response for column {question_config.data_column_name}; skipping...")
                sync_stats.add_event(KoboToolBoxSyncEvents.FOUND_A_NULL_RESPONSE)
                continue
            sync_stats.add_event(KoboToolBoxSyncEvents.READ_ANSWER_FROM_RESPONSE)

            participant_uuid = _get_participant_uuid_for_response(form_response, kobotoolbox_source.sync_config.participant_id_configuration.id_type, 
                                                                  kobotoolbox_source.sync_config.participant_id_configuration.data_column_name, 
                                                                  uuid_table, kobotoolbox_source.sync_config)

            engagement_db_message = _form_answer_to_engagement_db_message(form_answer, kobotoolbox_source.sync_config.asset_uid, form_response, participant_uuid,
                                          question_config.engagement_db_dataset, question_config.data_column_name)

            message_origin_details = {"message_id": f"{form_response['_id']}_{form_response['formhub/uuid']}",
                                          "timestamp": form_response.get("_submission_time"),
                                          "text": form_answer}

            sync_event = _ensure_engagement_db_has_message(engagement_db, engagement_db_message, message_origin_details)
            sync_stats.add_event(sync_event)

            last_seen_response_time = form_response.get("_submission_time")

    if cache is not None and last_seen_response_time is not None:
        cache.set_date_time(kobotoolbox_source.sync_config.asset_uid, isoparse(last_seen_response_time))  

    return sync_stats


def sync_kobotoolbox_sources_to_engagement_db(google_cloud_credentials_file_path, kobotoolbox_sources, engagement_db,
                                              uuid_table, cache_path=None):
    """
    Syncs KoboToolBox Forms to an engagement database.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               download KoboToolBox Form credentials.
    :type google_cloud_credentials_file_path: str
    :param kobotoolbox_sources: Configuration for the KoboToolBox Forms to sync.
    :type kobotoolbox_sources: list of src.kobotoolbox_to_engagement_db.configuration.KoboToolBoxSource
    :param engagement_db: Engagement database to sync
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str | None
    """

    asset_uid_to_sync_stats = OrderedDict()
    all_sync_stats = KoboToolBoxToEngagementDBSyncStats()
    for i, form_source in enumerate(kobotoolbox_sources):
        log.info(f"Processing form configuration {i + 1}/{len(kobotoolbox_sources)}...")
        asset_uid = form_source.sync_config.asset_uid
        sync_stats = _sync_kobotoolbox_to_engagement_db(google_cloud_credentials_file_path, form_source, engagement_db,
                                              uuid_table, cache_path
                                              )
        asset_uid_to_sync_stats[asset_uid] = sync_stats
        all_sync_stats.add_stats(sync_stats)

    for asset_uid, sync_stats in asset_uid_to_sync_stats.items():
        log.info(f"Summary of actions for KoboToolBox Form '{asset_uid}':")
        sync_stats.print_summary()

    log.info(f"Summary of actions for all KoboToolBox Forms:")
    all_sync_stats.print_summary()
