CREATE TABLE [dbo].[s_hybris_enumeration_values] (
    [s_hybris_enumeration_values_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)      NOT NULL,
    [hjmpts]                         BIGINT         NULL,
    [created_ts]                     DATETIME       NULL,
    [modified_ts]                    DATETIME       NULL,
    [enumeration_values_pk]          BIGINT         NULL,
    [code]                           NVARCHAR (255) NULL,
    [code_lower_case]                NVARCHAR (255) NULL,
    [sequence_number]                INT            NULL,
    [p_extension_name]               NVARCHAR (255) NULL,
    [p_icon]                         BIGINT         NULL,
    [acl_ts]                         BIGINT         NULL,
    [prop_ts]                        BIGINT         NULL,
    [editable]                       TINYINT        NULL,
    [dv_load_date_time]              DATETIME       NOT NULL,
    [dv_r_load_source_id]            BIGINT         NOT NULL,
    [dv_inserted_date_time]          DATETIME       NOT NULL,
    [dv_insert_user]                 VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]           DATETIME       NULL,
    [dv_update_user]                 VARCHAR (50)   NULL,
    [dv_hash]                        CHAR (32)      NOT NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_enumeration_values]
    ON [dbo].[s_hybris_enumeration_values]([bk_hash] ASC, [s_hybris_enumeration_values_id] ASC);

