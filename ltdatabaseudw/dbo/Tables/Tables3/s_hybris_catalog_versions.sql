CREATE TABLE [dbo].[s_hybris_catalog_versions] (
    [s_hybris_catalog_versions_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [hjmpts]                       BIGINT         NULL,
    [created_ts]                   DATETIME       NULL,
    [modified_ts]                  DATETIME       NULL,
    [owner_pk_string]              BIGINT         NULL,
    [catalog_versions_pk]          BIGINT         NULL,
    [p_active]                     TINYINT        NULL,
    [p_version]                    NVARCHAR (255) NULL,
    [p_mime_root_directory]        NVARCHAR (255) NULL,
    [p_generation_date]            DATETIME       NULL,
    [p_default_currency]           BIGINT         NULL,
    [p_incl_freight]               TINYINT        NULL,
    [p_incl_packing]               TINYINT        NULL,
    [p_incl_assurance]             TINYINT        NULL,
    [p_incl_duty]                  TINYINT        NULL,
    [p_territories]                VARCHAR (8000) NULL,
    [p_languages]                  VARCHAR (8000) NULL,
    [p_generator_info]             NVARCHAR (255) NULL,
    [p_category_system_id]         NVARCHAR (255) NULL,
    [p_previous_update_version]    INT            NULL,
    [p_mnemonic]                   NVARCHAR (255) NULL,
    [acl_ts]                       BIGINT         NULL,
    [prop_ts]                      BIGINT         NULL,
    [dv_load_date_time]            DATETIME       NOT NULL,
    [dv_batch_id]                  BIGINT         NOT NULL,
    [dv_r_load_source_id]          BIGINT         NOT NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL,
    [dv_hash]                      CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_catalog_versions]
    ON [dbo].[s_hybris_catalog_versions]([bk_hash] ASC, [s_hybris_catalog_versions_id] ASC);

