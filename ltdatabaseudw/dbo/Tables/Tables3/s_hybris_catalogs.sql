CREATE TABLE [dbo].[s_hybris_catalogs] (
    [s_hybris_catalogs_id]   BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)      NOT NULL,
    [hjmpts]                 BIGINT         NULL,
    [created_ts]             DATETIME       NULL,
    [modified_ts]            DATETIME       NULL,
    [catalogs_pk]            BIGINT         NULL,
    [p_id]                   NVARCHAR (200) NULL,
    [p_default_catalog]      TINYINT        NULL,
    [p_preview_url_template] NVARCHAR (255) NULL,
    [acl_ts]                 BIGINT         NULL,
    [prop_ts]                BIGINT         NULL,
    [dv_load_date_time]      DATETIME       NOT NULL,
    [dv_batch_id]            BIGINT         NOT NULL,
    [dv_r_load_source_id]    BIGINT         NOT NULL,
    [dv_inserted_date_time]  DATETIME       NOT NULL,
    [dv_insert_user]         VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]   DATETIME       NULL,
    [dv_update_user]         VARCHAR (50)   NULL,
    [dv_hash]                CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_catalogs]
    ON [dbo].[s_hybris_catalogs]([bk_hash] ASC, [s_hybris_catalogs_id] ASC);

