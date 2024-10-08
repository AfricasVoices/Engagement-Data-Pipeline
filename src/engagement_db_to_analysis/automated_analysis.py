from core_data_modules.analysis import (engagement_counts, repeat_participations, theme_distributions, sample_messages,
                                        AnalysisConfiguration, traffic_analysis, cross_tabs)
from core_data_modules.analysis.mapping import participation_maps, kenya_mapper, somalia_mapper, mapping_utils
from core_data_modules.logging import Logger
from core_data_modules.util import IOUtils

from src.engagement_db_to_analysis.column_view_conversion import (analysis_dataset_configs_to_rqa_column_configs,
                                                                  analysis_dataset_configs_to_demog_column_configs,
                                                                  analysis_dataset_configs_to_column_configs)

from src.engagement_db_to_analysis.configuration import AnalysisLocations, MapConfiguration
from src.engagement_db_to_analysis.regression_analysis.complete_case_regression_analysis import \
    export_all_complete_case_regression_analysis_txt
from src.engagement_db_to_analysis.regression_analysis.multiple_imputation_regression_analysis import \
    export_all_multiple_imputation_regression_analysis_txt

log = Logger(__name__)

MAPPERS = {
    AnalysisLocations.KENYA_COUNTY: kenya_mapper.export_kenya_counties_map,
    AnalysisLocations.KENYA_CONSTITUENCY: kenya_mapper.export_kenya_constituencies_map,

    AnalysisLocations.MOGADISHU_SUB_DISTRICT: somalia_mapper.export_mogadishu_sub_district_frequencies_map,
    AnalysisLocations.SOMALIA_DISTRICT: somalia_mapper.export_somalia_district_frequencies_map,
    AnalysisLocations.SOMALIA_REGION: somalia_mapper.export_somalia_region_frequencies_map
}


def _get_column_config_with_dataset_name(dataset_name, column_configs):
    """
    Gets the column configuration with the given dataset_name.

    :param dataset_name: Dataset to look-up.
    :type dataset_name: str
    :param column_configs: Configurations to search.
    :type column_configs: list of core_data_modules.analysis.analysis_utils.AnalysisConfiguration
    :return: Configuration with dataset_name property `dataset_name`.
    :rtype: core_data_modules.analysis.analysis_utils.AnalysisConfiguration
    """
    for column_config in column_configs:
        if column_config.dataset_name == dataset_name:
            return column_config
    raise LookupError(dataset_name)


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
    all_column_configs = analysis_dataset_configs_to_column_configs(analysis_config.dataset_configurations)
    rqa_column_configs = analysis_dataset_configs_to_rqa_column_configs(analysis_config.dataset_configurations)
    demog_column_configs = analysis_dataset_configs_to_demog_column_configs(analysis_config.dataset_configurations)
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

    if analysis_config.cross_tabs is not None:
        for cross_tabs_config in analysis_config.cross_tabs:
            (cross_tab_dataset_1, cross_tab_dataset_2) = cross_tabs_config
            log.info(f"Exporting cross-tabs for {cross_tab_dataset_1} and {cross_tab_dataset_2}...")
            cross_tab_column_config_1 = _get_column_config_with_dataset_name(cross_tab_dataset_1, all_column_configs)
            cross_tab_column_config_2 = _get_column_config_with_dataset_name(cross_tab_dataset_2, all_column_configs)
            with open(f"{export_dir_path}/cross_tabs_{cross_tab_dataset_1}_vs_{cross_tab_dataset_2}.csv", "w") as f:
                cross_tabs.export_cross_tabs_csv(
                    participants_by_column, "consent_withdrawn", cross_tab_column_config_1, cross_tab_column_config_2, f
                )
    else:
        log.debug(f"Not exporting any cross-tabs because `analysis_config.cross_tabs` was None")

    log.info("Exporting up to 100 sample messages for each RQA code...")
    with open(f"{export_dir_path}/sample_messages.csv", "w") as f:
        sample_messages.export_sample_messages_csv(
            messages_by_column, "consent_withdrawn", rqa_column_configs, f, limit_per_code=100
        )

    if analysis_config.traffic_labels is not None:
        log.info("Exporting traffic analysis...")
        with open(f"{export_dir_path}/traffic_analysis.csv", "w") as f:
            traffic_analysis.export_traffic_analysis_csv(
                messages_by_column, "consent_withdrawn", rqa_column_configs, "timestamp",
                analysis_config.traffic_labels, f
            )
    else:
        log.debug("Not running any traffic analysis because analysis_configuration.traffic_labels is None")

    if analysis_config.enable_experimental_regression_analysis:
        log.info(f"Running experimental complete-case regression analysis...")
        with open(f"{export_dir_path}/complete_case_regression.txt", "w") as f:
            export_all_complete_case_regression_analysis_txt(
                participants_by_column, "consent_withdrawn", rqa_column_configs, demog_column_configs, f
            )

        log.info(f"Running experimental multiple-imputation regression analysis...")
        with open(f"{export_dir_path}/multiple_imputation_regression.txt", "w") as f:
            export_all_multiple_imputation_regression_analysis_txt(
                participants_by_column, "consent_withdrawn", rqa_column_configs, demog_column_configs, f
            )

    log.info(f"Exporting participation maps for each location dataset...")
    mappers = {
        AnalysisLocations.KENYA_COUNTY: kenya_mapper.export_kenya_counties_map,
        AnalysisLocations.KENYA_CONSTITUENCY: kenya_mapper.export_kenya_constituencies_map,

        AnalysisLocations.MOGADISHU_SUB_DISTRICT: somalia_mapper.export_mogadishu_sub_district_frequencies_map,
        AnalysisLocations.SOMALIA_DISTRICT: somalia_mapper.export_somalia_district_frequencies_map,
        AnalysisLocations.SOMALIA_REGION: somalia_mapper.export_somalia_region_frequencies_map
    }
    
    map_configurations = analysis_config.maps
    if map_configurations is None:
        map_configurations = []
        for analysis_dataset_config in analysis_config.dataset_configurations:
            for coding_config in analysis_dataset_config.coding_configs:
                if coding_config.analysis_location in MAPPERS:
                    map_configurations.append(MapConfiguration(coding_config.analysis_location))

    log.info(f"Exporting participation maps for locations "
             f"{[config.analysis_location for config in map_configurations]}...")

    for map_config in map_configurations:
        dataset_config, coding_config = analysis_config.get_configurations_for_analysis_location(
            map_config.analysis_location
        )

        column_config = AnalysisConfiguration(
            dataset_name=coding_config.analysis_dataset,
            raw_field=dataset_config.raw_dataset,
            coded_field=f"{coding_config.analysis_dataset}_labels",
            code_scheme=coding_config.code_scheme
        )

        participation_maps.export_participation_maps(
            participants_by_column, "consent_withdrawn", rqa_column_configs, column_config,
            lambda x, y: (MAPPERS[coding_config.analysis_location](
                x, y, region_filter=map_config.region_filter, legend_position=map_config.legend_position)
            ),
            f"{export_dir_path}/maps/{column_config.dataset_name}/{column_config.dataset_name}_"
        )
