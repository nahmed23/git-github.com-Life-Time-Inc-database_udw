﻿CREATE TABLE [dbo].[s_boss_product_format] (
    [s_boss_product_format_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)     NOT NULL,
    [product_format_id]        INT           NULL,
    [short_desc]               CHAR (15)     NULL,
    [long_desc]                CHAR (50)     NULL,
    [help_text]                VARCHAR (240) NULL,
    [jan_one]                  DATETIME      NULL,
    [dv_load_date_time]        DATETIME      NOT NULL,
    [dv_r_load_source_id]      BIGINT        NOT NULL,
    [dv_inserted_date_time]    DATETIME      NOT NULL,
    [dv_insert_user]           VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]     DATETIME      NULL,
    [dv_update_user]           VARCHAR (50)  NULL,
    [dv_hash]                  CHAR (32)     NOT NULL,
    [dv_deleted]               BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]              BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_product_format]([dv_batch_id] ASC);

