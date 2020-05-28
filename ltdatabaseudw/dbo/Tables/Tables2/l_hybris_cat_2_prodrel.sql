﻿CREATE TABLE [dbo].[l_hybris_cat_2_prodrel] (
    [l_hybris_cat_2_prodrel_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)    NOT NULL,
    [hjmpts]                    BIGINT       NULL,
    [type_pk_string]            BIGINT       NULL,
    [pk]                        BIGINT       NULL,
    [owner_pk_string]           BIGINT       NULL,
    [source_pk]                 BIGINT       NULL,
    [target_pk]                 BIGINT       NULL,
    [sequence_number]           INT          NULL,
    [language_pk]               BIGINT       NULL,
    [dv_load_date_time]         DATETIME     NOT NULL,
    [dv_r_load_source_id]       BIGINT       NOT NULL,
    [dv_inserted_date_time]     DATETIME     NOT NULL,
    [dv_insert_user]            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]      DATETIME     NULL,
    [dv_update_user]            VARCHAR (50) NULL,
    [dv_hash]                   CHAR (32)    NOT NULL,
    [dv_batch_id]               BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_cat_2_prodrel]
    ON [dbo].[l_hybris_cat_2_prodrel]([bk_hash] ASC, [l_hybris_cat_2_prodrel_id] ASC);

