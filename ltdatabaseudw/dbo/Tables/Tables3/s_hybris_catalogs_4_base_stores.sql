CREATE TABLE [dbo].[s_hybris_catalogs_4_base_stores] (
    [s_hybris_catalogs_4_base_stores_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)      NOT NULL,
    [hjmpts]                             BIGINT         NULL,
    [created_ts]                         DATETIME       NULL,
    [modified_ts]                        DATETIME       NULL,
    [catalogs_4_base_stores_pk]          BIGINT         NULL,
    [qualifier]                          NVARCHAR (255) NULL,
    [sequence_number]                    INT            NULL,
    [r_sequence_number]                  INT            NULL,
    [acl_ts]                             BIGINT         NULL,
    [prop_ts]                            BIGINT         NULL,
    [dv_load_date_time]                  DATETIME       NOT NULL,
    [dv_r_load_source_id]                BIGINT         NOT NULL,
    [dv_inserted_date_time]              DATETIME       NOT NULL,
    [dv_insert_user]                     VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]               DATETIME       NULL,
    [dv_update_user]                     VARCHAR (50)   NULL,
    [dv_hash]                            CHAR (32)      NOT NULL,
    [dv_batch_id]                        BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_catalogs_4_base_stores]
    ON [dbo].[s_hybris_catalogs_4_base_stores]([bk_hash] ASC, [s_hybris_catalogs_4_base_stores_id] ASC);

