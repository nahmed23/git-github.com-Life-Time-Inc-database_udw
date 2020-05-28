CREATE TABLE [dbo].[d_medallia_survey_data] (
    [d_medallia_survey_data_id]  BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)      NOT NULL,
    [d_medallia_survey_data_key] VARCHAR (32)   NULL,
    [survey_id]                  DECIMAL (15)   NULL,
    [field_name]                 VARCHAR (255)  NULL,
    [field_value]                VARCHAR (8000) NULL,
    [file_name]                  VARCHAR (255)  NULL,
    [p_medallia_survey_data_id]  BIGINT         NOT NULL,
    [deleted_flag]               INT            NULL,
    [dv_load_date_time]          DATETIME       NULL,
    [dv_load_end_date_time]      DATETIME       NULL,
    [dv_batch_id]                BIGINT         NOT NULL,
    [dv_inserted_date_time]      DATETIME       NOT NULL,
    [dv_insert_user]             VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]       DATETIME       NULL,
    [dv_update_user]             VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

