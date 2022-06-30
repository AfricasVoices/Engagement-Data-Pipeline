from core_data_modules.cleaners import swahili

from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="AIK-ELECTIONS",
    test_participant_uuids=[
        "avf-participant-uuid-ced0f78f-f989-42e4-8813-b1ead4fff0ae",
        "avf-participant-uuid-2fa53f8c-8f71-490c-8aff-40c6929b2675",
        "avf-participant-uuid-7d817591-37b9-43ef-b3c3-303fdfa1544f",
        "avf-participant-uuid-88ef05ba-4c56-41f8-a00c-29104abab73e",
        "avf-participant-uuid-b972d5f2-be30-4eea-be1c-a4d973a15330",
        "avf-participant-uuid-7d144e9e-b54f-4d1f-bca9-3e8cdeeaedcc",
        "avf-participant-uuid-73b4f00a-5e49-418d-896f-95185f59fe4d",
        "avf-participant-uuid-f24811c3-90a6-4fcd-bf32-e8b38eb98ca4"
    ],
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/experimental_test"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_test",
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
                form_id="1FAIpQLSfKLNStjszOx8j_NNUUx_ARqHxrx7n2ZbuuhlO2CB_DmcNV2A",
                participant_id_configuration=ParticipantIdConfiguration(
                    question_title="What is your phone number",
                    id_type=GoogleFormParticipantIdTypes.KENYA_MOBILE_NUMBER
                ),
                question_configurations=[
                    # Demographic Questions
                    QuestionConfiguration(engagement_db_dataset="aik_language", question_title=["We could either do the interview in English or Swahili. Which language would you prefer?"]),
                    QuestionConfiguration(engagement_db_dataset="pool_kenya_age", question_title=["Do you mind telling me how old you are?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_education", question_title=["What is the highest level of education attained ?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_employment_status", question_title=["What is your employment Status ?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_religion", question_title=["What is your religion ?"]),
                    QuestionConfiguration(engagement_db_dataset="pool_kenya_gender", question_title=["What is your sex? "]),
                    QuestionConfiguration(engagement_db_dataset="aik_household income", question_title=["Approximately what is your gross monthly household income? (I.e. This is the combined monthly income of all your household members). This will help us in determining your social-economic class."]),
                    QuestionConfiguration(engagement_db_dataset="pool_kenya_disabled", question_title=["Do you have any form of disability? (If disability is visible, do not ask, make the judgement)"]),
                    QuestionConfiguration(engagement_db_dataset="aik_community", question_title=["What community do you belong to?", "Is it considered indigenous or minority?  if yes provide details."]),
                    QuestionConfiguration(engagement_db_dataset="pool_kenya_location", question_title=["Can I presume that you are currently a resident of; …………… County? [mention name of the target county]", 
                                                                                                       "Can I presume that you are currently a resident of; …………… Sub-County / Constituency? [mention name of the target sub-county]",
                                                                                                       "Can I presume that you are currently a resident of; …………… Ward? [mention name of the target ward]"]),
                    ## GENERAL ELECTORAL ENVIRONMENT
                    QuestionConfiguration(engagement_db_dataset="aik_voting_participation", question_title=["Do you plan on voting in the August 9th General Elections?", "If NOT, why are you not planning to vote? "]),
                    QuestionConfiguration(engagement_db_dataset="aik_political_participation", question_title=["Do you feel comfortable participating in any political activities in your area of residence?", 
                                                                                                               "Reasons for your answer on political activities participation."]),
                    QuestionConfiguration(engagement_db_dataset="aik_political_environment", question_title=["Do you think the political and security environment is conducive to free and fair elections?"]),

                    ## HATE SPEECH AND INCITEMENT 
                    QuestionConfiguration(engagement_db_dataset="aik_election_conversations", question_title=["In your view, have elections-related conversations become more controversial and conflictual in the past two weeks than the two weeks before?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_hate_speech_and_actions_target", question_title=["Have you heard comments or seen actions motivated by hatred/negative attitudes regarding a person's identity in the last two weeks?", 
                                                                                                                      "If YES, What did they target?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_identity_groups_increase", question_title=["In your view, has there been an increase in groups with strong political identities challenging others with different loyalties?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_political_events_disruption", question_title=["In your area, has there been an increase in disruption of political events by the opponent's supporters?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_intolerance_incidents", question_title=["In your area, has there been an increase in bullying, harassment, and general intolerance incidents?",
                                                                                                             "If YES, on what grounds?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_unsafe_areas", question_title=["Are there areas in your community that have become more unsafe in the last two weeks?", 
                                                                                                    "If YES, where and why are they unsafe?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_electoral_violence_anxiety", question_title=["Are you or your family/neighbours more worried about electoral violence than two weeks ago?", 
                                                                                                                  "If YES, why are they worried about electoral violence?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_electoral_sexual_gender_based_violence", question_title=["Are you or your family/neighbours more worried about electoral gender-based violence than two weeks ago?", 
                                                                                                                              "If YES, why are they worried about electoral gender-based violence?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_willingness_to_help_victims", question_title=["Would you be willing to help a neighbour from a different political view or ethnic background if they were attacked?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_engaging_authorities", question_title=["Does your household know how to safely and quickly report a crime or seek help from the authorities?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_incitement_sources", question_title=["Which sources have you seen or heard hateful/inciteful statements about other communities, identities, and religions in the last two weeks?",
                                                                                                          "If there are any, what was the nature of the statements?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_vote_buying", question_title=["Have you heard or seen incidents of voters being encouraged not to vote or sell their voters cards?", 
                                                                                                   "If YES, when and where did this incidents of encouragement on not to vote happen?",
                                                                                                   "Who encouraged this?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_influence_on_voting_choices", question_title=["Based on the campaign activities over the past two weeks, what do you think will influence people's voting choices in your area?"]),
                    
                    ## RISK OF VIOLENCE & CONFLICT
                    QuestionConfiguration(engagement_db_dataset="aik_incidents_of_polarisation", question_title=["In the last two weeks, have some areas become no go areas for political supporters or ethnic groups.", 
                                                                                                                 "If YES, when and where did this happen, and who is being driven out?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_inability_to_work", question_title=["Are there community members who have not been able to work in the last two weeks?", 
                                                                                                         "If YES, why has this happened to those community members?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_incidents_of_violence_and_polarisation", question_title=["Have violent public protests or communal riots taken place?", 
                                                                                                                              "If YES, when, where and why did this public riots or communal riots happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_police_brutality", question_title=["Have police officers used excessive force and / or live ammunition to respond to protesters?", 
                                                                                                        "If YES, when and where did this incident on police brutality happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_vandalism_theft_incidents", question_title=["Have people's homes and assets been vandalized and/or stolen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_physical_harm", question_title=["Have there been injuries and deaths related to elections activities in the last two weeks?", 
                                                                                                     "If YES, when and where did this election physical harm happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_sexual_assault", question_title=["Have members of your community been sexually assaulted or raped related to elections activities in the last two weeks?", 
                                                                                                      "If YES, when and where did this sexual assault happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_violence_displacement", question_title=["Has violence displaced members of your community?", 
                                                                                                             "If YES, when and where did this incidents of displacement happen?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_concern_about_safety_and_security", question_title=["Based on the current political and security environment, are you concerned about safety and security within your community?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_peace_and_security_initiatives", question_title=["Have you heard of Initiatives aimed at enhancing peace and security in the last two weeks?", 
                                                                                                                      "If YES, what were the peace and security initiatives?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_iebc_effectiveness", question_title=["Independent Electoral and Boundaries Commission (IEBC)", "Add reason for the score on IEBC?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_nps_effectiveness", question_title=["National Police Service", "Add reason for the score on NPS?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_ncic_effectiveness", question_title=["National Cohesion and Integration Commission(NCIC)", "Add reason for the score on NCIC?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_dpp_effectiveness", question_title=["Office of the Director of Public Prosecutions", "Add reason for the score on DPP?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_ipoa_effectiveness", question_title=["Independent Policing Oversight Authority", "Add reason for the score on IPOA?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_judiciary_effectiveness", question_title=["The Judiciary", "Add reason for the score on Judiciary?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_knchr_effectiveness", question_title=["Kenya National Commission on Human Rights", "Add reason for the score on KNCHR?"]),
                    QuestionConfiguration(engagement_db_dataset="aik_other_institutions_effectiveness", question_title=["List other institutions and their ratings?"]),

                    ## PARTICIPATION IN FUTURE SURVEYS
                    QuestionConfiguration(engagement_db_dataset="", question_title=["Would you wish to participate in other surveys in future?"]),
                ]
            )
        )
    ],
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/AIK-ELECTIONS/"
    )
)