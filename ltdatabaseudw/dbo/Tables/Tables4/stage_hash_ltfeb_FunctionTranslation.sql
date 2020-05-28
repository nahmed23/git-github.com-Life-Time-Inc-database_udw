CREATE TABLE [dbo].[stage_hash_ltfeb_FunctionTranslation] (
    [stage_hash_ltfeb_FunctionTranslation_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)      NOT NULL,
    [function_name]                           NVARCHAR (159) NULL,
    [function_value]                          NVARCHAR (527) NULL,
    [function_value_table_name]               NVARCHAR (159) NULL,
    [update_datetime]                         DATETIME       NULL,
    [update_userid]                           NVARCHAR (31)  NULL,
    [dv_load_date_time]                       DATETIME       NOT NULL,
    [dv_inserted_date_time]                   DATETIME       NOT NULL,
    [dv_insert_user]                          VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                    DATETIME       NULL,
    [dv_update_user]                          VARCHAR (50)   NULL,
    [dv_batch_id]                             BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

