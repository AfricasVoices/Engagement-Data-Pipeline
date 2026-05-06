from core_data_modules.cleaners import swahili
from dateutil.parser import isoparse
from src.pipeline_configuration_spec import *


def make_analysis_dataset_config(dataset_name, dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER):
    return AnalysisDatasetConfiguration(
        engagement_db_datasets=[dataset_name.strip()],
        dataset_type=dataset_type,
        raw_dataset=f"{dataset_name}_raw".strip(),
        coding_configs=[
            CodingConfiguration(
                code_scheme=load_code_scheme(dataset_name.strip()),
                analysis_dataset=dataset_name.strip()
            )
        ]
    )

def make_rqa_coda_dataset_config(coda_dataset_id):
    return CodaDatasetConfiguration(
        coda_dataset_id=coda_dataset_id.strip(),
        engagement_db_dataset=coda_dataset_id.lower().strip(),
        code_scheme_configurations=[
            CodeSchemeConfiguration(code_scheme=load_code_scheme(coda_dataset_id.lower().strip()),
                                    auto_coder=None, coda_code_schemes_count=3),
        ],
        ws_code_match_value=coda_dataset_id.lower().strip()
    )

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="Global Fund",
    test_participant_uuids=[],
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
    google_form_sources=[
        GoogleFormSource(
            google_form_client=GoogleFormsClientConfiguration(
                credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json"
            ),
            sync_config=GoogleFormToEngagementDBConfiguration(
                form_id="116JuidEgKiT3f0Qhp8dvLpFtVia_0Wr-ud5V1IuWL4o",
                question_configurations=[
                    # RQAs
                    QuestionConfiguration(question_ids=["279f382c"], engagement_db_dataset="judiciary_general_perception"),
                    QuestionConfiguration(question_ids=["138b8e5f"], engagement_db_dataset="judiciary_trust_aspects"),
                    QuestionConfiguration(question_ids=["19010aee"], engagement_db_dataset="judiciary_improvements"),

                    QuestionConfiguration(question_ids=["232aef3e"], engagement_db_dataset="judiciary_frequent_challenges"),
                    QuestionConfiguration(question_ids=["6d56517f"], engagement_db_dataset="judiciary_challenge_implications"),

                    QuestionConfiguration(question_ids=["4995747b"], engagement_db_dataset="judiciary_reforms_needed"),
                    QuestionConfiguration(question_ids=["60a23f4c"], engagement_db_dataset="judiciary_bridge_the_gap"),
                    QuestionConfiguration(question_ids=["21d4514e"], engagement_db_dataset="judiciary_priority_reform"),

                    # Demogs
                    QuestionConfiguration(question_ids=["2bc7d007"], engagement_db_dataset="age"),
                    QuestionConfiguration(question_ids=["46b733bc"], engagement_db_dataset="gender"),
                    QuestionConfiguration(question_ids=["26d37c17"], engagement_db_dataset="location"),
                    QuestionConfiguration(question_ids=["7f0a3296"], engagement_db_dataset="disability"),
                    QuestionConfiguration(question_ids=["57785ec1"], engagement_db_dataset="disability_type"),
                    QuestionConfiguration(question_ids=["294685b5"], engagement_db_dataset="judiciary_law_court"),
                ]
            )
        ),
    ],
    kobotoolbox_sources=[
        KoboToolBoxSource(
            token_file_url="gs://avf-credentials/uraia-kobotoolbox-token.json",
            sync_config=KoboToolBoxToEngagementDBConfiguration(
                asset_uid="aHh6nR5ADxkzWCp4ZSqFqr",
                participant_id_configuration=KoboToolBoxParticipantIdConfiguration(
                    data_column_name="group_mx8ue07/What_is_your_phone_n_yako_ya_simu_ni_gani",
                    id_type=KoboToolBoxParticipantIdTypes.KENYA_MOBILE_NUMBER
                ),
                ignore_invalid_mobile_numbers=True,
                question_configurations=[
                    KoboToolBoxQuestionConfiguration(data_column_name="group_wj5hl79/What_specific_experi_a_huduma_za_mahakama", engagement_db_dataset="judiciary_specific_experiences"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_wj5hl79/How_would_you_descri_ahakama_haswa_raiya", engagement_db_dataset="judiciary_treatment_by_court_staff"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_wj5hl79/To_what_extent_do_yo_ali_elezea_jibu_lako", engagement_db_dataset="judiciary_extent_of_transparency"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_wj5hl79/In_your_opinion_wha_ell_or_have_improved", engagement_db_dataset="judiciary_aspects_working_well"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_qb1ef19/What_barriers_finan_a_huduma_za_mahakama", engagement_db_dataset="judiciary_barriers_to_access"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_qb1ef19/In_your_opinion_how_a_huduma_za_mahakama", engagement_db_dataset="judiciary_challenges_influence_trust"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_fl5wo14/If_you_could_recomme_uwa_lipi_na_kwa_nini", engagement_db_dataset="judiciary_recommend_priority_reform"),

                    KoboToolBoxQuestionConfiguration(data_column_name="group_dp2wm27/What_is_you_age_Je_umri_wako_ni_upi", engagement_db_dataset="age"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_go9un15/group_dp2wm27/What_is_your_gender_jinsia_yako_ni_ipi", engagement_db_dataset="gender"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_dp2wm27/What_is_your_locatio_shi_wapi_eneo_bunge", engagement_db_dataset="location"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_dp2wm27/Do_you_have_a_disabi_una_ulemavu_wowote", engagement_db_dataset="disabled"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_dp2wm27/What_court_s_have_y_kutumia_huduma_zake", engagement_db_dataset="judiciary_law_court"),
                ]
            )
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                make_rqa_coda_dataset_config("judiciary_general_perception"),
                make_rqa_coda_dataset_config("judiciary_trust_aspects"),
                make_rqa_coda_dataset_config("judiciary_improvements"),
                make_rqa_coda_dataset_config("judiciary_frequent_challenges"),
                make_rqa_coda_dataset_config("judiciary_challenge_implications"),
                make_rqa_coda_dataset_config("judiciary_reforms_needed"),
                make_rqa_coda_dataset_config("judiciary_bridge_the_gap"),
                make_rqa_coda_dataset_config("judiciary_priority_reform"),
                make_rqa_coda_dataset_config("disability_type"),
                make_rqa_coda_dataset_config("judiciary_law_court"),

                make_rqa_coda_dataset_config("judiciary_specific_experiences"),
                make_rqa_coda_dataset_config("judiciary_treatment_by_court_staff"),
                make_rqa_coda_dataset_config("judiciary_extent_of_transparency"),
                make_rqa_coda_dataset_config("judiciary_aspects_working_well"),
                make_rqa_coda_dataset_config("judiciary_barriers_to_access"),
                make_rqa_coda_dataset_config("judiciary_challenges_influence_trust"),
                make_rqa_coda_dataset_config("judiciary_recommend_priority_reform"),

                CodaDatasetConfiguration(
                    coda_dataset_id="Kenya_Pool_location",
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
                    coda_dataset_id="Kenya_Pool_preferred_language",
                    engagement_db_dataset="preferred_language",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/preferred_language"), 
                                                auto_coder=None, coda_code_schemes_count=3)
                    ],
                    ws_code_match_value="preferred_language"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2022/POOL-KENYA/pool-kenya-users.json",
            default_ws_dataset="kenya_pool_old_rqa_datasets"
        )
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="global_fund_s03_analysis_output"
        ),
        dataset_configurations=[
            make_analysis_dataset_config("judiciary_general_perception"),
            make_analysis_dataset_config("judiciary_trust_aspects"),
            make_analysis_dataset_config("judiciary_improvements"),
            make_analysis_dataset_config("judiciary_frequent_challenges"),
            make_analysis_dataset_config("judiciary_challenge_implications"),
            make_analysis_dataset_config("judiciary_reforms_needed"),
            make_analysis_dataset_config("judiciary_bridge_the_gap"),
            make_analysis_dataset_config("judiciary_priority_reform"),
            make_analysis_dataset_config("disability_type"),
            make_analysis_dataset_config("judiciary_law_court"),

            make_analysis_dataset_config("judiciary_specific_experiences"),
            make_analysis_dataset_config("judiciary_treatment_by_court_staff"),
            make_analysis_dataset_config("judiciary_extent_of_transparency"),
            make_analysis_dataset_config("judiciary_aspects_working_well"),
            make_analysis_dataset_config("judiciary_barriers_to_access"),
            make_analysis_dataset_config("judiciary_challenges_influence_trust"),
            make_analysis_dataset_config("judiciary_recommend_priority_reform"),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["preferred_language"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="preferred_language_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/preferred_language"),
                        analysis_dataset="preferred_language"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["gender"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="gender_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/gender"),
                        analysis_dataset="gender"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["disabled"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="disabled_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/disabled"),
                        analysis_dataset="disabled"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                 engagement_db_datasets=["location"],
                 dataset_type=DatasetTypes.DEMOGRAPHIC,
                 raw_dataset="location_raw",
                 coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/kenya_county"),
                        analysis_dataset="kenya_county",
                        analysis_location=AnalysisLocations.KENYA_COUNTY
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/kenya_constituency"),
                        analysis_dataset="kenya_constituency",
                        analysis_location=AnalysisLocations.KENYA_CONSTITUENCY
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/kenya_ward"),
                        analysis_dataset="kenya_ward",
                        analysis_location=AnalysisLocations.KENYA_WARD
                    )
                 ]
             ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["age"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="age_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/age"),
                        analysis_dataset="age"
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/age_category"),
                        analysis_dataset="age_category",
                        age_category_config=AgeCategoryConfiguration(
                            age_analysis_dataset="age",
                            categories={
                                (10, 14): "10 to 14",
                                (15, 17): "15 to 17",
                                (18, 35): "18 to 35",
                                (36, 54): "36 to 54",
                                (55, 99): "55 to 99"
                            }
                        )
                    ),
                ],
            )
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
        maps=[],
        traffic_labels=[]
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2024/judiciary_pilot/"
    )
)
