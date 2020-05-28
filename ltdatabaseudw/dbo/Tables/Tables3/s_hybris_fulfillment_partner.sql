CREATE TABLE [dbo].[s_hybris_fulfillment_partner] (
    [s_hybris_fulfillment_partner_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)      NOT NULL,
    [hjmpts]                          BIGINT         NULL,
    [fulfillment_partner_pk]          BIGINT         NULL,
    [created_ts]                      DATETIME       NULL,
    [modified_ts]                     DATETIME       NULL,
    [acl_ts]                          INT            NULL,
    [prop_ts]                         INT            NULL,
    [p_display_name]                  NVARCHAR (255) NULL,
    [p_code]                          NVARCHAR (255) NULL,
    [p_ftp_to]                        NVARCHAR (255) NULL,
    [p_ftp_from]                      NVARCHAR (255) NULL,
    [p_work_day_supplier_id]          NVARCHAR (255) NULL,
    [p_inventory_to]                  NVARCHAR (255) NULL,
    [p_inventory_file_format]         NVARCHAR (255) NULL,
    [p_sender_qualifier]              NVARCHAR (255) NULL,
    [p_receiver_qualifier]            NVARCHAR (255) NULL,
    [dv_load_date_time]               DATETIME       NOT NULL,
    [dv_r_load_source_id]             BIGINT         NOT NULL,
    [dv_inserted_date_time]           DATETIME       NOT NULL,
    [dv_insert_user]                  VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]            DATETIME       NULL,
    [dv_update_user]                  VARCHAR (50)   NULL,
    [dv_hash]                         CHAR (32)      NOT NULL,
    [dv_batch_id]                     BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_fulfillment_partner]
    ON [dbo].[s_hybris_fulfillment_partner]([bk_hash] ASC, [s_hybris_fulfillment_partner_id] ASC);

