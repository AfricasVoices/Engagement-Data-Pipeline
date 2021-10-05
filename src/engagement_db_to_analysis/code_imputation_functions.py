from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.cleaners.location_tools import KenyaLocations
from core_data_modules.data_models import Label, Origin
from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.util import TimeUtils
from engagement_database.data_models import Message

from src.engagement_db_to_analysis.column_view_conversion import (analysis_dataset_configs_to_column_configs,
                                                                  analysis_dataset_configs_to_demog_column_configs)
from src.engagement_db_to_analysis.column_view_conversion import (get_latest_labels_with_code_scheme,
                                                                  analysis_dataset_config_for_message)
from src.pipeline_configuration_spec import *

log = Logger(__name__)


def _clear_latest_labels(user, message_td, code_schemes):
    message = Message.from_dict(dict(message_td))
    code_scheme_ids = [code_scheme.scheme_id for code_scheme in code_schemes]
    for label in message.get_latest_labels():
        cleared_label = None
        for scheme_id in code_scheme_ids:
            if label.scheme_id.startswith(scheme_id):
                cleared_label = Label(
                    label.scheme_id,
                    "SPECIAL-MANUALLY_UNCODED",
                    TimeUtils.utc_now_as_iso_string(),
                    Origin(Metadata.get_call_location(), "Engagement DB -> Analysis", "External")
                )
        assert cleared_label is not None
        _insert_label_to_message_td(user, message_td, cleared_label)


def _insert_label_to_message_td(user, message_traced_data, label):
    """
    Inserts a new label to the list of labels for this message, and writes-back to TracedData.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param message_traced_data: Message TracedData objects to impute age_category.
    :type message_traced_data: TracedData
    :param label: New label to insert to the message_traced_data
    :type: core_data_modules.data_models.Label
    """
    label = label.to_dict()
    message_labels = message_traced_data["labels"].copy()
    message_labels.insert(0, label)
    message_traced_data.append_data(
        {"labels": message_labels},
        Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))


def _impute_not_reviewed_labels(user, messages_traced_data, analysis_dataset_configs, ws_correct_dataset_code_scheme):
    """
    Imputes Codes.NOT_REVIEWED label for messages that have not been manually checked in coda.

    A message is considered to be manually checked if it contains only labels which are checked. Messages that fall
    into this case will not be modified.

    If a message contains only unchecked labels (or no labels), the labels will be replaced with Codes.NOT_REVIEWED.

    If a message contains a mix of checked and unchecked labels, the labels will be replaced with Codes.CODING_ERROR.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute not reviewed labels.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_configs: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    :param ws_correct_dataset_code_scheme: WS - Correct Dataset code scheme.
    :type ws_correct_dataset_code_scheme: core_data_modules.data_models.CodeScheme
    """

    log.info(f"Imputing {Codes.NOT_REVIEWED} labels...")
    messages_with_nr_imputed = 0
    messages_with_ce_imputed = 0
    for message_td in messages_traced_data:
        message = Message.from_dict(dict(message_td))

        message_analysis_config = analysis_dataset_config_for_message(analysis_dataset_configs, message)

        # Check if the message has a manual label and impute NOT_REVIEWED if it doesn't
        has_checked_label = False
        has_unchecked_label = False
        code_schemes = [c.code_scheme for c in message_analysis_config.coding_configs]
        code_schemes.append(ws_correct_dataset_code_scheme)
        for code_scheme in code_schemes:
            latest_labels_with_code_scheme = get_latest_labels_with_code_scheme(
                message, code_scheme
            )
            for label in latest_labels_with_code_scheme:
                if label.checked:
                    has_checked_label = True
                else:
                    has_unchecked_label = True

        if has_checked_label and not has_unchecked_label:
            # Message is exclusively manually reviewed
            continue

        if has_checked_label and has_unchecked_label:
            # The message has been partially reviewed. Map this to coding error.
            _clear_latest_labels(user, message_td, code_schemes)

            for code_scheme in code_schemes:
                coding_error_label = CleaningUtils.make_label_from_cleaner_code(
                    code_scheme, code_scheme.get_code_with_control_code(Codes.CODING_ERROR),
                    Metadata.get_call_location())

                # Insert a coding error label to the list of labels for this message, and write-back to TracedData.
                _insert_label_to_message_td(user, message_td, coding_error_label)
            messages_with_ce_imputed += 1
            continue

        # Label has not been manually reviewed at all, so replace the codes with Codes.NOT_REVIEWED
        assert not has_checked_label
        _clear_latest_labels(user, message_td, code_schemes)
        for code_scheme in code_schemes:
            not_reviewed_label = CleaningUtils.make_label_from_cleaner_code(
                code_scheme, code_scheme.get_code_with_control_code(Codes.NOT_REVIEWED),
                Metadata.get_call_location())

            # Insert not_reviewed_label to the list of labels for this message, and write-back to TracedData.
            _insert_label_to_message_td(user, message_td, not_reviewed_label)
        messages_with_nr_imputed += 1

    log.info(f"Processed {Codes.NOT_REVIEWED} labels for {len(messages_traced_data)} messages traced data. "
             f"Imputed {Codes.NOT_REVIEWED} labels for {messages_with_nr_imputed} messages, and "
             f"imputed {Codes.CODING_ERROR} labels for {messages_with_ce_imputed} messages")


def _code_for_label(label, code_schemes):
    """
    Returns the code for the given label.

    Handles duplicated scheme ids (i.e. schemes ending in '-1', '-2' etc.).
    Raises a ValueError if the label isn't for any of the given code schemes.

    :param label: Label to get the code for.
    :type label: core_data_modules.data_models.Label
    :param code_schemes: Code schemes to check for the given label.
    :type code_schemes: list of core_data_modules.data_models.CodeScheme
    :return: Code for the label.
    :rtype: core_data_modules.data_models.Code
    """
    for code_scheme in code_schemes:
        if label.scheme_id.startswith(code_scheme.scheme_id):
            return code_scheme.get_code_with_code_id(label.code_id)

    raise ValueError(f"Label's scheme id '{label.scheme_id}' is not in any of the given `code_schemes` "
                     f"(these have ids {[scheme.scheme_id for scheme in code_schemes]})")


def _impute_ws_coding_errors(user, messages_traced_data, analysis_dataset_configs, ws_correct_dataset_code_scheme):
    """
    Imputes Codes.CODING_ERROR labels for messages that have a coding error in the WS labels that have been applied.

    We consider WS labels to have a coding error if either of these conditions holds:
     - There is a WS label applied in a normal code scheme, but there is no label in the WS - Correct Dataset code scheme.
     - There is a label applied in the WS - Correct Dataset code scheme, but no WS code in any of the normal code schemes.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute ws coding errors.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_configs: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    :param ws_correct_dataset_code_scheme: WS - Correct Dataset code scheme.
    :type ws_correct_dataset_code_scheme: core_data_modules.data_models.CodeScheme
    """
    log.info(f"Imputing {Codes.CODING_ERROR} labels for WS codes...")
    imputed_labels = 0
    for message_td in messages_traced_data:
        message = Message.from_dict(dict(message_td))

        message_analysis_config = analysis_dataset_config_for_message(analysis_dataset_configs, message)
        normal_code_schemes = [c.code_scheme for c in message_analysis_config.coding_configs]

        # Check for a WS code in any of the normal code schemes
        ws_code_in_normal_scheme = False
        for label in message.get_latest_labels():
            if not label.checked:
                continue

            if label.scheme_id != ws_correct_dataset_code_scheme.scheme_id:
                code = _code_for_label(label, normal_code_schemes)
                if code.control_code == Codes.WRONG_SCHEME:
                    ws_code_in_normal_scheme = True

        # Check for a code in the WS code scheme
        code_in_ws_scheme = False
        for label in message.get_latest_labels():
            if not label.checked:
                continue

            if label.scheme_id == ws_correct_dataset_code_scheme.scheme_id:
                code_in_ws_scheme = True

        if ws_code_in_normal_scheme != code_in_ws_scheme:
            imputed_labels += 1

            # Clear all existing labels, in preparation for the new coding error labels we'll write afterwards.
            # (This is because messages store labels in Coda format, so before we write the new labels we need to
            #  insert special un-coded labels in place of all the existing labels, including labels assigned under
            #  duplicate schemes, in order to guarantee that no pre-existing label is preserved in the next steps of
            #  analysis)
            _clear_latest_labels(user, message_td, normal_code_schemes + [ws_correct_dataset_code_scheme])

            # Append a CE code under every normal + WS code scheme
            for code_scheme in normal_code_schemes + [ws_correct_dataset_code_scheme]:
                ce_label = CleaningUtils.make_label_from_cleaner_code(
                    code_scheme,
                    code_scheme.get_code_with_control_code(Codes.CODING_ERROR),
                    Metadata.get_call_location(),
                    set_checked=True
                )
                _insert_label_to_message_td(user, message_td, ce_label)

    log.info(f"Imputed {imputed_labels} {Codes.CODING_ERROR} labels for WS codes")


def _impute_age_category(user, messages_traced_data, analysis_dataset_configs):
    """
    Imputes age category for age dataset messages.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute age_category.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_configs: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """

    # Get the coding configurations for age and age_category analysis datasets
    age_category_coding_config = None
    for analysis_dataset_config in analysis_dataset_configs:
        for coding_config in analysis_dataset_config.coding_configs:
            if coding_config.age_category_config is None:
                log.info(f"No age_category config in {coding_config.analysis_dataset} skipping...")
                continue

            log.info(f"Found age_category in {coding_config.analysis_dataset} coding config")
            assert age_category_coding_config is None, f"Found more than one age_category configs, expected one, crashing"
            age_category_coding_config = coding_config

    if age_category_coding_config is None:
        log.info(f"No age category configuration found, returning without imputing any age categories")
        return

    age_coding_config = None
    age_engagement_db_datasets = None
    for analysis_dataset_config in analysis_dataset_configs:
        for coding_config in analysis_dataset_config.coding_configs:
            if coding_config.analysis_dataset == age_category_coding_config.age_category_config.age_analysis_dataset:

                assert age_coding_config is None, f"Found more than one age_coding_config in analysis_dataset_config," \
                    f"expected one, crashing"
                age_coding_config = coding_config
                age_engagement_db_datasets = analysis_dataset_config.engagement_db_datasets

    # Check and impute age_category in age messages only
    log.info(f"Imputing {age_category_coding_config.analysis_dataset} labels for {age_coding_config.analysis_dataset} messages...")
    imputed_labels = 0
    age_messages = 0
    for message_td in messages_traced_data:
        if message_td["dataset"] in age_engagement_db_datasets:
            age_messages += 1

            age_labels = get_latest_labels_with_code_scheme(Message.from_dict(dict(message_td)), age_coding_config.code_scheme)
            age_code = age_coding_config.code_scheme.get_code_with_code_id(age_labels[0].code_id)

            # Impute age_category for this age_code
            if age_code.code_type == CodeTypes.NORMAL:
                age_category = None
                for age_range, category in age_category_coding_config.age_category_config.categories.items():
                    if age_range[0] <= age_code.numeric_value <= age_range[1]:
                        age_category = category
                assert age_category is not None
                age_category_code = age_category_coding_config.code_scheme.get_code_with_match_value(age_category)
            elif age_code.code_type == CodeTypes.META:
                age_category_code = age_category_coding_config.code_scheme.get_code_with_meta_code(age_code.meta_code)
            else:
                assert age_code.code_type == CodeTypes.CONTROL
                age_category_code = age_category_coding_config.code_scheme.get_code_with_control_code(
                    age_code.control_code)

            age_category_label = CleaningUtils.make_label_from_cleaner_code(
                age_category_coding_config.code_scheme, age_category_code, Metadata.get_call_location()
            )

            # Inserts this age_category_label to the list of labels for this message, and write-back to TracedData.
            _insert_label_to_message_td(user, message_td, age_category_label)

            imputed_labels += 1

    log.info(f"Imputed {imputed_labels} age category labels for {age_messages} age messages")


def _make_location_code(scheme, clean_value):
    if clean_value == Codes.NOT_CODED:
        return scheme.get_code_with_control_code(Codes.NOT_CODED)
    else:
        return scheme.get_code_with_match_value(clean_value)


def _impute_kenya_location_codes(user, messages_traced_data, analysis_dataset_configs):
    """
    Imputes Kenya location labels for location dataset messages.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute age_category.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_configs: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """
    log.info(f"Imputing Kenya location labels for location messages...")

    # Get the coding configurations for constituency and county analysis datasets
    constituency_coding_config = None
    county_coding_config = None
    location_engagement_db_datasets = None
    for analysis_dataset_config in analysis_dataset_configs:
        for coding_config in analysis_dataset_config.coding_configs:
            if coding_config.kenya_analysis_location == AnalysisLocations.KENYA_CONSTITUENCY:
                log.info(f"Found kenya_analysis_location in county {coding_config.analysis_dataset} coding config")

                assert constituency_coding_config is None, f"Found more than one constituency_coding_config in " \
                    f"analysis_dataset_config, expected one crashing"
                constituency_coding_config = coding_config
                location_engagement_db_datasets = analysis_dataset_config.engagement_db_datasets

            elif coding_config.kenya_analysis_location == AnalysisLocations.KENYA_COUNTY:
                log.info(f"Found kenya_analysis_location in constituency {coding_config.analysis_dataset} coding config")

                assert county_coding_config is None, f"Found more than one county_coding_config in " \
                    f"analysis_dataset_config, expected one crashing"
                county_coding_config = coding_config

    if constituency_coding_config is not None and county_coding_config is not None:
        imputed_normal_labels = 0
        imputed_meta_labels = 0
        imputed_control_labels = 0
        detected_coding_errors = 0

        for message_traced_data in messages_traced_data:
            message = Message.from_dict(dict(message_traced_data))
            if message.dataset in location_engagement_db_datasets:
                message_analysis_config = analysis_dataset_config_for_message(analysis_dataset_configs, message)

                # Up to 1 location code should have been assigned in Coda. Search for that code,
                # ensuring that only 1 has been assigned or, if multiple have been assigned, that they are non-conflicting control codes
                # Multiple normal codes will be converted to Coding Error, even if they were compatible (e.g. langata + nairobi)
                location_code = None
                for coding_config in message_analysis_config.coding_configs:
                    latest_coding_config_labels = get_latest_labels_with_code_scheme(message, coding_config.code_scheme)

                    if len(latest_coding_config_labels) > 0:
                        latest_coding_config_label = latest_coding_config_labels[0]

                        coda_code = coding_config.code_scheme.get_code_with_code_id(latest_coding_config_label.code_id)
                        if location_code is not None:
                            if location_code.code_id != coda_code.code_id:
                                location_code = constituency_coding_config.code_scheme.get_code_with_control_code(
                                    Codes.CODING_ERROR
                                )
                                detected_coding_errors += 1
                        else:
                            location_code = coda_code

                # If a control or meta code was found, set all other location keys to that control/meta code,
                # otherwise convert the provided location to the other locations in the hierarchy.
                if location_code.code_type == CodeTypes.CONTROL:
                    for coding_config in message_analysis_config.coding_configs:
                        control_code_label = CleaningUtils.make_label_from_cleaner_code(
                            coding_config.code_scheme,
                            coding_config.code_scheme.get_code_with_control_code(location_code.control_code),
                            Metadata.get_call_location())

                        _insert_label_to_message_td(user, message_traced_data, control_code_label)
                        imputed_control_labels += 1
                elif location_code.code_type == CodeTypes.META:
                    for coding_config in message_analysis_config.coding_configs:
                        meta_code_label = CleaningUtils.make_label_from_cleaner_code(
                            coding_config.code_scheme,
                            coding_config.code_scheme.get_code_with_meta_code(location_code.meta_code),
                            Metadata.get_call_location())

                        _insert_label_to_message_td(user, message_traced_data, meta_code_label)
                        imputed_meta_labels += 1
                else:
                    location = location_code.match_values[0]
                    constituency_label = CleaningUtils.make_label_from_cleaner_code(
                        constituency_coding_config.code_scheme,
                        _make_location_code(constituency_coding_config.code_scheme,
                                            KenyaLocations.constituency_for_location_code(location)),
                        Metadata.get_call_location()
                    )

                    county_label = CleaningUtils.make_label_from_cleaner_code(
                        county_coding_config.code_scheme,
                        _make_location_code(
                            county_coding_config.code_scheme,
                            KenyaLocations.county_for_location_code(location)
                        ),
                        Metadata.get_call_location()
                    )

                    _insert_label_to_message_td(user, message_traced_data, constituency_label)
                    _insert_label_to_message_td(user, message_traced_data, county_label)
                    imputed_normal_labels += 1

        log.info(f"Detected {detected_coding_errors} coding errors, and imputed {imputed_normal_labels} normal, "
                 f"{imputed_meta_labels} meta, and {imputed_control_labels} control Kenyan location labels.")
    else:
        assert county_coding_config is None or constituency_coding_config is None
        log.warning("Missing location coding_config(s) in analysis_dataset_config, skipping imputing location labels...")


def impute_codes_by_message(user, messages_traced_data, analysis_dataset_configs, ws_correct_dataset_code_scheme):
    """
    Imputes codes for messages TracedData in-place.

    Runs the following imputations:
     - Imputes Codes.NOT_REVIEWED for messages that have not been manually labelled in coda.
     - Imputes Age category labels for age dataset messages.
     - Imputes Kenya Location labels for location dataset messages.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute age_category.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_configs: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    :param ws_correct_dataset_code_scheme: WS - Correct Dataset code scheme.
    :type ws_correct_dataset_code_scheme: core_data_modules.data_models.CodeScheme
    """
    _impute_not_reviewed_labels(user, messages_traced_data, analysis_dataset_configs, ws_correct_dataset_code_scheme)
    _impute_ws_coding_errors(user, messages_traced_data, analysis_dataset_configs, ws_correct_dataset_code_scheme)
    _impute_age_category(user, messages_traced_data, analysis_dataset_configs)
    _impute_kenya_location_codes(user, messages_traced_data, analysis_dataset_configs)


def _impute_true_missing(user, column_traced_data_iterable, analysis_dataset_configs):
    """
    Imputes TRUE_MISSING codes on column-view datasets.

    TRUE_MISSING labels are applied to analysis datasets where the raw dataset doesn't exist in the given TracedData.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param column_traced_data_iterable: Column-view traced data objects to apply the impute function to.
    :type column_traced_data_iterable: iterable of core_data_modules.traced_data.TracedData
    :param analysis_dataset_configs: Analysis dataset configurations for the imputation.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """
    imputed_codes = 0
    log.info(f"Imputing {Codes.TRUE_MISSING} codes...")

    column_configs = analysis_dataset_configs_to_column_configs(analysis_dataset_configs)

    for td in column_traced_data_iterable:
        na_dict = dict()

        for column_config in column_configs:
            if column_config.raw_field in td:
                continue

            na_dict[column_config.raw_field] = ""
            na_label = CleaningUtils.make_label_from_cleaner_code(
                column_config.code_scheme,
                column_config.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                Metadata.get_call_location()
            ).to_dict()
            na_dict[column_config.coded_field] = [na_label]
            imputed_codes += 1

        td.append_data(na_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    log.info(f"Imputed {imputed_codes} {Codes.TRUE_MISSING} codes for {len(column_traced_data_iterable)} "
             f"traced data items")


def _demog_has_conflicting_normal_labels(column_traced_data, column_config):
    """
    :param column_traced_data: Column-view traced data to check.
    :type column_traced_data: core_data_modules.traced_data.TracedData
    :param column_config: Configuration for the demographic column to analyse.
    :type column_config: core_data_modules.analysis.analysis_utils.AnalysisConfiguration
    :return: Whether there are any conflicting normal labels for `column_traced_data` under `column_config`.
    :rtype: bool
    """
    column_labels = column_traced_data[column_config.coded_field]
    normal_code = None
    for label in column_labels:
        code = column_config.code_scheme.get_code_with_code_id(label["CodeID"])
        if code.code_type == CodeTypes.NORMAL:
            if normal_code is None:
                normal_code = code

            if normal_code.code_id != code.code_id:
                return True

    return False


def _get_control_and_meta_labels(column_traced_data, column_config):
    """
    :param column_traced_data: Column-view traced data to get the control and meta labels from.
    :type column_traced_data: core_data_modules.traced_data.TracedData
    :param column_config: Configuration for the column to get the labels from.
    :type column_config: core_data_modules.analysis.analysis_utils.AnalysisConfiguration
    :return: The labels in `column_traced_data` that have code_type CONTROL or META under `column_config`, serialized.
    :rtype: list of dict
    """
    control_and_meta_labels = []
    column_labels = column_traced_data[column_config.coded_field]
    for label in column_labels:
        code = column_config.code_scheme.get_code_with_code_id(label["CodeID"])
        if code.code_type == CodeTypes.CONTROL or code.code_type == CodeTypes.META:
            control_and_meta_labels.append(label)
    return control_and_meta_labels


def _impute_nic_demogs(user, column_traced_data_iterable, analysis_dataset_configs):
    """
    Imputes NOT_INTERNALLY_CONSISTENT labels on the demographic columns of column-view datasets.

    NOT_INTERNALLY_CONSISTENT labels are applied to demographics where there are multiple, conflicting normal labels.
    For example:
     - If we have multiple conflicting normal labels e.g. "20" and "22", this would be converted to
       NOT_INTERNALLY_CONSISTENT.
     - If we have a a single normal age label and some meta/control labels, no imputation is performed.
     - If we have multiple normal age labels but these have the same code id, e.g. for the messages "21" and "I'm 21",
       no imputation is performed.
     - If we have multiple conflicting normal labels, e.g. "20" and "22", as well as some meta/control labels, the
       normal labels will be replaced with NOT_INTERNALLY_CONSISTENT and the meta/control labels will be kept untouched.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param column_traced_data_iterable: Column-view traced data objects to apply the impute function to.
    :type column_traced_data_iterable: iterable of core_data_modules.traced_data.TracedData
    :param analysis_dataset_configs: Analysis dataset configurations for the imputation.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """
    imputed_codes = 0
    log.info(f"Imputing {Codes.NOT_INTERNALLY_CONSISTENT} codes...")

    demog_column_configs = analysis_dataset_configs_to_demog_column_configs(analysis_dataset_configs)
    for td in column_traced_data_iterable:
        for column_config in demog_column_configs:
            if _demog_has_conflicting_normal_labels(td, column_config):
                # Replace the conflicting normal labels with a NOT_INTERNALLY_CONSISTENT label,
                # while keeping any existing control/meta codes.
                new_labels = _get_control_and_meta_labels(td, column_config)
                nic_label = CleaningUtils.make_label_from_cleaner_code(
                    column_config.code_scheme,
                    column_config.code_scheme.get_code_with_control_code(Codes.NOT_INTERNALLY_CONSISTENT),
                    Metadata.get_call_location()
                )
                new_labels.append(nic_label.to_dict())

                td.append_data(
                    {column_config.coded_field: new_labels},
                    Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
                )
                imputed_codes += 1

    log.info(f"Imputed {imputed_codes} {Codes.NOT_INTERNALLY_CONSISTENT} codes for {len(column_traced_data_iterable)} "
             f"traced data items")


def _get_consent_withdrawn_participant_uuids(column_traced_data_iterable, analysis_dataset_configs):
    """
    Gets the participant uuids of participants who withdrew consent.

    A participant is considered to have withdrawn consent if any of their labels have control code Codes.STOP in any
    of the datasets in the given `analysis_dataset_configs`.

    :param column_traced_data_iterable: Column-view traced data objects to search for consent withdrawn status.
    :type column_traced_data_iterable: iterable of core_data_modules.traced_data.TracedData
    :param analysis_dataset_configs: Analysis dataset configurations for the search.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    :return: Uuids of participants who withdrew consent.
    :rtype: set of str
    """
    column_configs = analysis_dataset_configs_to_column_configs(analysis_dataset_configs)
    consent_withdrawn_uuids = set()

    for td in column_traced_data_iterable:
        for column_config in column_configs:
            column_labels = td[column_config.coded_field]
            for label in column_labels:
                if column_config.code_scheme.get_code_with_code_id(label["CodeID"]).control_code == Codes.STOP:
                    consent_withdrawn_uuids.add(td["participant_uuid"])

    return consent_withdrawn_uuids


def _impute_consent_withdrawn(user, column_traced_data_iterable, analysis_dataset_configs):
    """
    Imputes consent_withdrawn on column-view datasets.

    Searches the given data for participants who are labelled Codes.STOP under any of the given
    `analysis_dataset_configs`.

    If the participant withdrew consent:
     - Imputes {consent_withdrawn: Codes.TRUE}
     - Overwrites all labels with a STOP label
     - Overwrites all raw texts with "STOP".
    If the participant did not withdraw consent:
     - Imputes {consent_withdrawn: Codes.FALSE}

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param column_traced_data_iterable: Column-view traced data objects to apply the impute function to.
    :type column_traced_data_iterable: iterable of core_data_modules.traced_data.TracedData
    :param analysis_dataset_configs: Analysis dataset configurations for the imputation.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """
    log.info("Imputing consent withdrawn...")
    consent_withdrawn_uuids = _get_consent_withdrawn_participant_uuids(column_traced_data_iterable, analysis_dataset_configs)
    log.info(f"Found {len(consent_withdrawn_uuids)} participants who withdrew consent")

    column_configs = analysis_dataset_configs_to_column_configs(analysis_dataset_configs)
    consent_withdrawn_tds = 0
    for td in column_traced_data_iterable:
        if td["participant_uuid"] in consent_withdrawn_uuids:
            consent_withdrawn_dict = {"consent_withdrawn": Codes.TRUE}
            consent_withdrawn_tds += 1
            # Overwrite the labels and raw fields with STOP labels/texts.
            for column_config in column_configs:
                consent_withdrawn_dict[column_config.coded_field] = [CleaningUtils.make_label_from_cleaner_code(
                    column_config.code_scheme,
                    column_config.code_scheme.get_code_with_control_code(Codes.STOP),
                    Metadata.get_call_location()
                ).to_dict()]
                consent_withdrawn_dict[column_config.raw_field] = "STOP"
        else:
            consent_withdrawn_dict = {"consent_withdrawn": Codes.FALSE}
        td.append_data(consent_withdrawn_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    log.info(f"Imputed consent withdrawn for {len(column_traced_data_iterable)} traced data items - "
             f"{len(consent_withdrawn_uuids)} items were marked as consent_withdrawn")


def impute_codes_by_column_traced_data(user, column_traced_data_iterable, analysis_dataset_configs):
    """
    Imputes codes for column-view TracedData in-place.

    Runs the following imputations:
     - Imputes Codes.TRUE_MISSING to columns that don't have a raw_field entry.
     - Imputes Codes.NOT_INTERNALLY_CONSISTENT to demographic columns that have multiple conflicting normal codes.
     - Imputes consent_withdrawn.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param column_traced_data_iterable: Column-view traced data objects to apply the impute function to.
    :type column_traced_data_iterable: iterable of core_data_modules.traced_data.TracedData
    :param analysis_dataset_configs: Analysis dataset configurations for the imputation.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """
    _impute_true_missing(user, column_traced_data_iterable, analysis_dataset_configs)
    _impute_nic_demogs(user, column_traced_data_iterable, analysis_dataset_configs)
    _impute_consent_withdrawn(user, column_traced_data_iterable, analysis_dataset_configs)
