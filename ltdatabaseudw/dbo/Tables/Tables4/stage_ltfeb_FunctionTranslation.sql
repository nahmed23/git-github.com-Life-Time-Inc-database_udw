CREATE TABLE [dbo].[stage_ltfeb_FunctionTranslation] (
    [stage_ltfeb_FunctionTranslation_id] BIGINT         NOT NULL,
    [function_name]                      NVARCHAR (159) NULL,
    [function_value]                     NVARCHAR (527) NULL,
    [function_value_table_name]          NVARCHAR (159) NULL,
    [update_datetime]                    DATETIME       NULL,
    [update_userid]                      NVARCHAR (31)  NULL,
    [dv_batch_id]                        BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

