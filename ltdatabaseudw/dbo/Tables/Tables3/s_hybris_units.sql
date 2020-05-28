CREATE TABLE [dbo].[s_hybris_units] (
    [s_hybris_units_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [hjmpts]                BIGINT          NULL,
    [created_ts]            DATETIME        NULL,
    [modified_ts]           DATETIME        NULL,
    [units_pk]              BIGINT          NULL,
    [p_code]                NVARCHAR (255)  NULL,
    [p_conversion]          DECIMAL (26, 6) NULL,
    [p_unit_type]           NVARCHAR (255)  NULL,
    [acl_ts]                BIGINT          NULL,
    [prop_ts]               BIGINT          NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_units]
    ON [dbo].[s_hybris_units]([bk_hash] ASC, [s_hybris_units_id] ASC);

