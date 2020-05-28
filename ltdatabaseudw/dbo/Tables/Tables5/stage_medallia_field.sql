CREATE TABLE [dbo].[stage_medallia_field] (
    [stage_medallia_field_id]  BIGINT         NOT NULL,
    [name_in_medallia]         VARCHAR (4000) NULL,
    [sr_no]                    VARCHAR (4000) NULL,
    [name_in_api]              VARCHAR (4000) NULL,
    [variable_name]            VARCHAR (4000) NULL,
    [answer_id]                VARCHAR (4000) NULL,
    [description_question]     VARCHAR (4000) NULL,
    [data_type]                VARCHAR (4000) NULL,
    [single_select]            VARCHAR (4000) NULL,
    [examples]                 VARCHAR (4000) NULL,
    [dummy_modified_date_time] DATETIME       NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

