CREATE TABLE [dbo].[fact_medallia_survey_data] (
    [fact_medallia_survey_data_id]          BIGINT         IDENTITY (1, 1) NOT NULL,
    [dim_club_key]                          VARCHAR (32)   NULL,
    [dim_medallia_field_key]                VARCHAR (255)  NULL,
    [dim_mms_member_key]                    VARCHAR (32)   NULL,
    [dim_mms_membership_key]                VARCHAR (32)   NULL,
    [dim_survey_created_dim_date_key]       VARCHAR (8)    NULL,
    [dim_survey_created_dim_time_key]       INT            NULL,
    [fact_medallia_survey_data_key]         VARCHAR (32)   NULL,
    [field_name]                            VARCHAR (255)  NULL,
    [file_name]                             VARCHAR (255)  NULL,
    [survey_data]                           VARCHAR (8000) NULL,
    [survey_data_converted_to_dim_date_key] VARCHAR (8)    NULL,
    [survey_data_converted_to_dim_time_key] INT            NULL,
    [survey_id]                             DECIMAL (15)   NULL,
    [survey_status]                         VARCHAR (255)  NULL,
    [survey_type]                           VARCHAR (255)  NULL,
    [dv_load_date_time]                     DATETIME       NULL,
    [dv_load_end_date_time]                 DATETIME       NULL,
    [dv_batch_id]                           BIGINT         NOT NULL,
    [dv_inserted_date_time]                 DATETIME       NOT NULL,
    [dv_insert_user]                        VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                  DATETIME       NULL,
    [dv_update_user]                        VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_medallia_survey_data_key]));

