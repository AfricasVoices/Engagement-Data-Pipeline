from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="Pool-Kenya-Demographics-To-TextIt",
    description="Syncs the latest demographics data from pool kenya to TextIt",
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
    rapid_pro_target=RapidProTarget(
        rapid_pro=RapidProClientConfiguration(
            domain="textit.com",
            token_file_url="gs://avf-credentials/pool-kenya-textit-token.txt"
        ),
        sync_config=EngagementDBToRapidProConfiguration(
            normal_datasets=[
                DatasetConfiguration(
                    engagement_db_datasets=["age"],
                    rapid_pro_contact_field=ContactField(key="pool_kenya_age", label="pool kenya age")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["gender"],
                    rapid_pro_contact_field=ContactField(key="pool_kenya_gender", label="pool kenya gender")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["location"],
                    rapid_pro_contact_field=ContactField(key="pool_kenya_location", label="pool kenya location")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["disabled"],
                    rapid_pro_contact_field=ContactField(key="pool_kenya_disabled", label="pool kenya disabled")
                ),
            ],
            consent_withdrawn_dataset=DatasetConfiguration(
                engagement_db_datasets=[
                    # ---------------------- Demogs -----------------------------
                    "age", "gender", "location", "disabled", "preferred_language",
                    # ------------------------------------- ICL ---------------------------------------------------
                    "machakos_governor_poll_2022", "machakos_senator_poll_2022", "machakos_womenrep_poll_2022_rita", 
                    "machakos_womenrep_poll_2022", "machakos_county_priorities_2022", "icl_pool_invitation_2022", 
                    "machakos_poll_registration_2022",
                    # ------------------------------------ Amnesty ----------------------------------------------------
                    "aik_household_income", "aik_communities", "aik_religion", "aik_employment_status", "aik_education", 
                    "aik_pool_invitation_2022", "aik_voting_participation", "aik_indigenous_or_minority", "aik_sexual_assault",
                    "aik_violence_displacement", "aik_vandalism_theft_incidents", "aik_inability_to_work", "aik_unsafe_areas",
                    "aik_political_events_disruption", "aik_political_environment", "aik_electoral_violence_anxiety",
                    "aik_police_brutality", "aik_physical_harm", "aik_peace_and_security_initiatives", "aik_engaging_authorities",
                    "aik_intolerance_incidents",  "aik_identity_groups_increase", "aik_hate_speech_and_actions_target",
                    "aik_source_of_vote_buying", "aik_incitement_sources", "aik_other_institutions_effectiveness", 
                    "aik_knchr_effectiveness", "aik_judiciary_effectiveness", "aik_ipoa_effectiveness", "aik_dpp_effectiveness", 
                    "aik_ncic_effectiveness", "aik_nps_effectiveness", "aik_iebc_effectiveness", "aik_incidents_of_polarisation",
                    "aik_willingness_to_help_victims", "aik_voting_participation", "aik_response_to_electoral_related_insecurity", 
                    "aik_electoral_sexual_gender_based_violence", "aik_concern_about_safety_and_security", "aik_election_conversations",
                    "aik_influence_on_voting_choices", "aik_political_participation",
                     # ------------------------------------- GIZ -------------------------------------
                    "giz_s01e01", "giz_s01e02", "giz_s01e03", "giz_s01e04", "giz_s01e05", "giz_s01e06",
                    # ------------------------------------ Porticus ---------------------------------------------
                    "porticus_s01e01", "porticus_s01e02", "porticus_s01e03", "porticus_s01e04", "porticus_s01e05",
                    # -------- Old rqa ----------
                    "kenya_pool_old_rqa_datasets"
                ],
                rapid_pro_contact_field=ContactField(key="pool_kenya_consent_withdrawn", label="pool kenya consent withdrawn")
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS,
            allow_clearing_fields=True,
            sync_advert_contacts=False,
        )     
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2023/Pool-Kenya-Demographics-To-TextIt"
    ),
)
