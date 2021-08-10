from core_data_modules.util import TimeUtils

from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.logging import Logger
from src.engagement_db_to_analysis.column_view_conversion import get_latest_labels_with_code_scheme
from engagement_database.data_models import Message


log = Logger(__name__)

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
    age_category_cc = None
    for analysis_dataset_config in analysis_dataset_configs:
        for coding_config in analysis_dataset_config.coding_configs:
            if coding_config.age_category_config is None:
                log.info(f"No age_category config in {coding_config.analysis_dataset} skipping...")
                continue

            log.info(f"Found age_category in {coding_config.analysis_dataset} coding config")
            assert age_category_cc is None, f"Found more than one age_category configs"
            age_category_cc = coding_config

    age_coding_config = None
    age_engagement_db_datasets = None
    for analysis_dataset_config in analysis_dataset_configs:
        for coding_config in analysis_dataset_config.coding_configs:
            if coding_config.analysis_dataset == age_category_cc.age_category_config.age_analysis_dataset:

                assert age_coding_config is None, f"Found more than one age_coding_config in analysis_dataset_config"
                age_coding_config = coding_config
                age_engagement_db_datasets = analysis_dataset_config.engagement_db_datasets

    # Check and impute age_category in age messages only
    log.info(f"Imputing {age_category_cc.analysis_dataset} labels for {age_coding_config.analysis_dataset} messages...")
    imputed_labels = 0
    age_messages = 0
    for message in messages_traced_data:
        if message["dataset"] in age_engagement_db_datasets:

            age_labels = get_latest_labels_with_code_scheme(Message.from_dict(dict(message)), age_coding_config.code_scheme)
            age_code = age_coding_config.code_scheme.get_code_with_code_id(age_labels[0].code_id)

            # Impute age_category for this age_code
            if age_code.code_type == CodeTypes.NORMAL:
                age_category = None
                for age_range, category in age_category_cc.age_category_config.categories.items():
                    if age_range[0] <= age_code.numeric_value <= age_range[1]:
                        age_category = category
                assert age_category is not None
                age_category_code = age_category_cc.code_scheme.get_code_with_match_value(age_category)
            elif age_code.code_type == CodeTypes.META:
                age_category_code = age_category_cc.code_scheme.get_code_with_meta_code(age_code.meta_code)
            else:
                assert age_code.code_type == CodeTypes.CONTROL
                age_category_code = age_category_cc.code_scheme.get_code_with_control_code(
                    age_code.control_code)

            age_category_label = CleaningUtils.make_label_from_cleaner_code(
                age_category_cc.code_scheme, age_category_code, Metadata.get_call_location()
            )

            # Append this age_category_label to the list of labels for this message, and write-back to TracedData.
            message_labels = message["labels"].copy()
            message_labels.insert(0, age_category_label.to_dict())
            message.append_data(
                {"labels": message_labels},
                Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
            )

            age_messages +=1
            imputed_labels +=1

    log.info(f"Imputed {imputed_labels} age category labels for {age_messages} age messages")


def impute_codes_by_message(user, messages_traced_data, analysis_dataset_configs):
    """
    Imputes codes for messages TracedData in-place.

    Runs the following imputations:
     - Imputes Age category labels for age dataset messages.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute age_category.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_configs: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """

    _impute_age_category(user, messages_traced_data, analysis_dataset_configs)
