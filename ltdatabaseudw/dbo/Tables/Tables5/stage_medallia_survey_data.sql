CREATE TABLE [dbo].[stage_medallia_survey_data] (
    [stage_medallia_survey_data_id] BIGINT         NOT NULL,
    [survey_id]                     DECIMAL (15)   NULL,
    [field_name]                    VARCHAR (255)  NULL,
    [field_value]                   VARCHAR (8000) NULL,
    [file_name]                     VARCHAR (255)  NULL,
    [dummy_modified_date_time]      DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

