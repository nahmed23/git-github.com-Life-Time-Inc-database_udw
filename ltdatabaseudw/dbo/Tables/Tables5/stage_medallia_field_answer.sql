CREATE TABLE [dbo].[stage_medallia_field_answer] (
    [stage_medallia_field_answer_id] BIGINT         NOT NULL,
    [answer_id]                      VARCHAR (4000) NULL,
    [answer_name]                    VARCHAR (4000) NULL,
    [answer_type]                    VARCHAR (4000) NULL,
    [dummy_modified_date_time]       DATETIME       NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

