﻿CREATE TABLE [dbo].[s_hybris_categories_lp] (
    [s_hybris_categories_lp_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [item_pk]                   BIGINT         NULL,
    [lang_pk]                   BIGINT         NULL,
    [p_description]             VARCHAR (8000) NULL,
    [p_name]                    NVARCHAR (255) NULL,
    [dv_load_date_time]         DATETIME       NOT NULL,
    [dv_r_load_source_id]       BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL,
    [dv_hash]                   CHAR (32)      NOT NULL,
    [dv_batch_id]               BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_categories_lp]
    ON [dbo].[s_hybris_categories_lp]([bk_hash] ASC, [s_hybris_categories_lp_id] ASC);

