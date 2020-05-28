CREATE TABLE [dbo].[s_ltfeb_function_translation] (
    [s_ltfeb_function_translation_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)      NOT NULL,
    [function_name]                   NVARCHAR (159) NULL,
    [function_value]                  NVARCHAR (527) NULL,
    [function_value_table_name]       NVARCHAR (159) NULL,
    [update_date_time]                DATETIME       NULL,
    [update_user_id]                  NVARCHAR (31)  NULL,
    [dv_load_date_time]               DATETIME       NOT NULL,
    [dv_r_load_source_id]             BIGINT         NOT NULL,
    [dv_inserted_date_time]           DATETIME       NOT NULL,
    [dv_insert_user]                  VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]            DATETIME       NULL,
    [dv_update_user]                  VARCHAR (50)   NULL,
    [dv_hash]                         CHAR (32)      NOT NULL,
    [dv_batch_id]                     BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ltfeb_function_translation]
    ON [dbo].[s_ltfeb_function_translation]([bk_hash] ASC, [s_ltfeb_function_translation_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ltfeb_function_translation]([dv_batch_id] ASC);

