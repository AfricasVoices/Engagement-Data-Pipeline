from core_data_modules.cleaners import swahili

from src.pipeline_configuration_spec import *

rapid_pro_uuid_filter = UuidFilter(
    uuid_file_url="gs://avf-project-datasets/2022/POOL-KENYA/pool-kenya-initial-de-identified-uuids.json"
)

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="Create-Initial-Kenya-Pool",
    description="Creates the initial Kenya Pool from demographics responses to COVID19, COVID19-Ke-Urban, "
                "UNDP-Kenya, UNICEF-KENYA, OXFAM-KENYA, World Vision, GPSDD and KE polls",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-engagement-databases-firebase-credentials-file.json",
        database_path="engagement_databases/POOL-KENYA"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-id-infrastructure-firebase-adminsdk-6xps8-b9173f2bfd.json",
        table_name="avf-global-urn-to-participant-uuid",
        uuid_prefix="avf-participant-uuid-"
    ),
    operations_dashboard=OperationsDashboardConfiguration(
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
    ),
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/covid19-2-text-it-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("covid19_ke_urban_s01_demog", "constituency", "location"),
                    FlowResultConfiguration("covid19_ke_urban_s01_demog", "gender", "gender"),
                    FlowResultConfiguration("covid19_ke_urban_s01_demog", "age", "age"),

                    FlowResultConfiguration("undp_kenya_s01_demog", "constituency", "location"),
                    FlowResultConfiguration("undp_kenya_s01_demog", "gender", "gender"),
                    FlowResultConfiguration("undp_kenya_s01_demog", "age", "age")
                ],
                uuid_filter=rapid_pro_uuid_filter
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/covid19-text-it-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("covid19_s01_demog", "constituency", "location"),
                    FlowResultConfiguration("covid19_s01_demog", "gender", "gender"),
                    FlowResultConfiguration("covid19_s01_demog", "age", "age"),
                ],
                uuid_filter=rapid_pro_uuid_filter
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/unicef-kenya-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("UNICEF_COVID19_KE_S01_Demog", "constituency", "location"),
                    FlowResultConfiguration("UNICEF_COVID19_KE_S01_Demog", "gender", "gender"),
                    FlowResultConfiguration("UNICEF_COVID19_KE_S01_Demog", "age", "age"),
                ],
                uuid_filter=rapid_pro_uuid_filter
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/oxfam-kenya-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("oxfam_wash_s01_demog", "constituency", "location"),
                    FlowResultConfiguration("oxfam_wash_s01_demog", "gender", "gender"),
                    FlowResultConfiguration("oxfam_wash_s01_demog", "age", "age"),
                    FlowResultConfiguration("oxfam_wash_s01_demog", "disabled", "disabled"),
                ],
                uuid_filter=rapid_pro_uuid_filter
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/world-vision-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("worldvision_s01_demog", "constituency", "location"),
                    FlowResultConfiguration("worldvision_s01_demog", "gender", "gender"),
                    FlowResultConfiguration("worldvision_s01_demog", "age", "age"),
                ],
                uuid_filter=rapid_pro_uuid_filter
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/gpsdd-kilifi-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("gpsdd_kilifi_s01_demog", "constituency", "location"),
                    FlowResultConfiguration("gpsdd_kilifi_s01_demog", "gender", "gender"),
                    FlowResultConfiguration("gpsdd_kilifi_s01_demog", "age", "age"),
                    FlowResultConfiguration("gpsdd_kilifi_s01_demog", "disabled", "disabled"),
                ],
                uuid_filter=rapid_pro_uuid_filter
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/gpsdd-kiambu-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("gpsdd_kiambu_s01_demog", "constituency", "location"),
                    FlowResultConfiguration("gpsdd_kiambu_s01_demog", "gender", "gender"),
                    FlowResultConfiguration("gpsdd_kiambu_s01_demog", "age", "age"),
                    FlowResultConfiguration("gpsdd_kiambu_s01_demog", "disabled", "disabled"),
                ],
                uuid_filter=rapid_pro_uuid_filter
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/gpsdd-bungoma-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("gpsdd_bungoma_s01_demog", "constituency", "location"),
                    FlowResultConfiguration("gpsdd_bungoma_s01_demog", "gender", "gender"),
                    FlowResultConfiguration("gpsdd_bungoma_s01_demog", "age", "age"),
                    FlowResultConfiguration("gpsdd_bungoma_s01_demog", "disabled", "disabled"),
                ],
                uuid_filter=rapid_pro_uuid_filter
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/ke-vax-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("ke_constitution_review_poll_demog", "constituency", "location"),
                    FlowResultConfiguration("ke_constitution_review_poll_demog", "gender", "gender"),
                    FlowResultConfiguration("ke_constitution_review_poll_demog", "age", "age"),
                    FlowResultConfiguration("ke_constitution_review_poll_demog", "disabled", "disabled"),
                ],
                uuid_filter=rapid_pro_uuid_filter
            )
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="Kenya_Pool_location", #TODO Rename as Pool_Kenya_location
                    engagement_db_dataset="location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/kenya_ward"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/kenya_constituency"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/kenya_county"), auto_coder=None)
                    ],
                    ws_code_match_value="location"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="Kenya_Pool_gender",
                    engagement_db_dataset="gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/gender"),
                                                auto_coder=swahili.DemographicCleaner.clean_gender)
                    ],
                    ws_code_match_value="gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="Kenya_Pool_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/age"), auto_coder=lambda x:
                                                str(swahili.DemographicCleaner.clean_age_within_range(x)))
                    ],
                    ws_code_match_value="age"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="Kenya_Pool_disabled",
                    engagement_db_dataset="disabled",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/disabled"), auto_coder=None)
                    ],
                    ws_code_match_value="disabled"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="Kenya_Pool_old_rqa_datasets",
                    engagement_db_dataset="kenya_pool_old_rqa_datasets",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("kenya_pool_old_rqa_datasets"), auto_coder=None)
                    ],
                    ws_code_match_value="kenya_pool_old_rqa_datasets"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2022/POOL-KENYA/pool-kenya-users.json",
            default_ws_dataset="kenya_pool_old_rqa_datasets"
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/CREATE-INITIAL-KENYA-POOL"
    )
)
