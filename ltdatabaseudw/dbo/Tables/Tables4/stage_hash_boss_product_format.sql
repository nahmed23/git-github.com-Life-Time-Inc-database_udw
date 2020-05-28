CREATE TABLE [dbo].[stage_hash_boss_product_format] (
    [stage_hash_boss_product_format_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)     NOT NULL,
    [id]                                INT           NULL,
    [short_desc]                        CHAR (15)     NULL,
    [long_desc]                         CHAR (50)     NULL,
    [help_text]                         VARCHAR (240) NULL,
    [jan_one]                           DATETIME      NULL,
    [dv_load_date_time]                 DATETIME      NOT NULL,
    [dv_inserted_date_time]             DATETIME      NOT NULL,
    [dv_insert_user]                    VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]              DATETIME      NULL,
    [dv_update_user]                    VARCHAR (50)  NULL,
    [dv_batch_id]                       BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

