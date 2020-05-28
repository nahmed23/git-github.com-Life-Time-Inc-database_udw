CREATE TABLE [dbo].[s_hybris_cat_2_prodrel] (
    [s_hybris_cat_2_prodrel_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [pk]                        BIGINT         NULL,
    [created_ts]                DATETIME       NULL,
    [modified_ts]               DATETIME       NULL,
    [acl_ts]                    INT            NULL,
    [prop_ts]                   INT            NULL,
    [qualifier]                 NVARCHAR (255) NULL,
    [r_sequence_number]         INT            NULL,
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
CREATE CLUSTERED INDEX [ci_s_hybris_cat_2_prodrel]
    ON [dbo].[s_hybris_cat_2_prodrel]([bk_hash] ASC, [s_hybris_cat_2_prodrel_id] ASC);

