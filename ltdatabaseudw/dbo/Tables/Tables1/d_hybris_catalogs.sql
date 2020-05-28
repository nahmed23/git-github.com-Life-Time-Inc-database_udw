CREATE TABLE [dbo].[d_hybris_catalogs] (
    [d_hybris_catalogs_id]     BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)      NOT NULL,
    [d_hybris_catalogs_key]    CHAR (32)      NULL,
    [catalogs_pk]              BIGINT         NULL,
    [acl_ts]                   INT            NULL,
    [created_ts]               DATETIME       NULL,
    [hjmpts]                   BIGINT         NULL,
    [modified_ts]              DATETIME       NULL,
    [owner_pk_string]          BIGINT         NULL,
    [p_active_catalog_version] BIGINT         NULL,
    [p_buyer]                  BIGINT         NULL,
    [p_default_catalog]        INT            NULL,
    [p_id]                     NVARCHAR (200) NULL,
    [p_preview_url_template]   NVARCHAR (200) NULL,
    [p_supplier]               BIGINT         NULL,
    [prop_ts]                  INT            NULL,
    [type_pk_string]           BIGINT         NULL,
    [p_hybris_catalogs_id]     BIGINT         NOT NULL,
    [dv_load_date_time]        DATETIME       NULL,
    [dv_load_end_date_time]    DATETIME       NULL,
    [dv_batch_id]              BIGINT         NOT NULL,
    [dv_inserted_date_time]    DATETIME       NOT NULL,
    [dv_insert_user]           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]     DATETIME       NULL,
    [dv_update_user]           VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

