CREATE TABLE [dbo].[s_hybris_consignments] (
    [s_hybris_consignments_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)      NOT NULL,
    [hjmpts]                   BIGINT         NULL,
    [consignments_pk]          BIGINT         NULL,
    [created_ts]               DATETIME       NULL,
    [modified_ts]              DATETIME       NULL,
    [acl_ts]                   INT            NULL,
    [prop_ts]                  INT            NULL,
    [p_tracking_id]            NVARCHAR (255) NULL,
    [p_shipping_date]          DATETIME       NULL,
    [p_named_delivery_date]    DATETIME       NULL,
    [p_code]                   NVARCHAR (255) NULL,
    [p_carrier]                NVARCHAR (255) NULL,
    [p_tracking_message]       NVARCHAR (255) NULL,
    [dv_load_date_time]        DATETIME       NOT NULL,
    [dv_r_load_source_id]      BIGINT         NOT NULL,
    [dv_inserted_date_time]    DATETIME       NOT NULL,
    [dv_insert_user]           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]     DATETIME       NULL,
    [dv_update_user]           VARCHAR (50)   NULL,
    [dv_hash]                  CHAR (32)      NOT NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_consignments]
    ON [dbo].[s_hybris_consignments]([bk_hash] ASC, [s_hybris_consignments_id] ASC);

