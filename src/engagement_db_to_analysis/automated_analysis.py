from core_data_modules.analysis import engagement_counts, repeat_participations, theme_distributions, sample_messages
from core_data_modules.logging import Logger
from core_data_modules.util import IOUtils

from src.engagement_db_to_analysis.column_view_conversion import analysis_dataset_config_to_column_configs
from src.engagement_db_to_analysis.configuration import DatasetTypes

log = Logger(__name__)


def _get_rqa_column_configs(analysis_config):
    rqa_column_configs = []
    for analysis_dataset_config in analysis_config.dataset_configurations:
        if analysis_dataset_config.dataset_type == DatasetTypes.RESEARCH_QUESTION_ANSWER:
            rqa_column_configs.extend(analysis_dataset_config_to_column_configs(analysis_dataset_config))
    return rqa_column_configs


def _get_demog_column_configs(analysis_config):
    demog_column_configs = []
    for analysis_dataset_config in analysis_config.dataset_configurations:
        if analysis_dataset_config.dataset_type == DatasetTypes.DEMOGRAPHIC:
            demog_column_configs.extend(analysis_dataset_config_to_column_configs(analysis_dataset_config))
    return demog_column_configs


def run_automated_analysis(messages_by_column, participants_by_column, analysis_config, export_dir_path):
    """
    Runs automated analysis and exports the results to disk.

    :param messages_by_column: Messages traced data in column-view format.
    :type messages_by_column: iterable of core_data_modules.traced_data.TracedData
    :param participants_by_column: Participants traced data in column-view format.
    :type participants_by_column: iterable of core_data_modules.traced_data.TracedData
    :param analysis_config: Configuration for the export.
    :type analysis_config: src.engagement_db_to_analysis.configuration.AnalysisConfiguration
    :param export_dir_path: Directory to export the automated analysis files to.
    :type export_dir_path: str
    """
    log.info(f"Running automated analysis...")
    rqa_column_configs = _get_rqa_column_configs(analysis_config)
    demog_column_configs = _get_demog_column_configs(analysis_config)
    IOUtils.ensure_dirs_exist(export_dir_path)

    log.info(f"Exporting engagement counts.csv...")
    with open(f"{export_dir_path}/engagement_counts.csv", "w") as f:
        engagement_counts.export_engagement_counts_csv(
            messages_by_column, participants_by_column, "consent_withdrawn", rqa_column_configs, f
        )

    log.info("Exporting repeat participations...")
    with open(f"{export_dir_path}/repeat_participations.csv", "w") as f:
        repeat_participations.export_repeat_participations_csv(
            participants_by_column, "consent_withdrawn", rqa_column_configs, f
        )

    log.info("Exporting theme distributions...")
    with open(f"{export_dir_path}/theme_distributions.csv", "w") as f:
        theme_distributions.export_theme_distributions_csv(
            participants_by_column, "consent_withdrawn", rqa_column_configs, demog_column_configs, f
        )

    log.info("Exporting demographic distributions...")
    with open(f"{export_dir_path}/demographic_distributions.csv", "w") as f:
        theme_distributions.export_theme_distributions_csv(
            participants_by_column, "consent_withdrawn", demog_column_configs, [], f
        )

    log.info("Exporting up to 100 sample messages for each RQA code...")
    with open(f"{export_dir_path}/sample_messages.csv", "w") as f:
        sample_messages.export_sample_messages_csv(
            messages_by_column, "consent_withdrawn", rqa_column_configs, f, limit_per_code=100
        )