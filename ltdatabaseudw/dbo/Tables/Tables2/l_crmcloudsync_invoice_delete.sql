CREATE TABLE [dbo].[l_crmcloudsync_invoice_delete] (
    [l_crmcloudsync_invoice_id] BIGINT         NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [account_id]                VARCHAR (36)   NULL,
    [contact_id]                VARCHAR (36)   NULL,
    [created_by]                VARCHAR (36)   NULL,
    [created_on_behalf_by]      VARCHAR (36)   NULL,
    [customer_id]               VARCHAR (36)   NULL,
    [entity_image_id]           VARCHAR (36)   NULL,
    [invoice_id]                VARCHAR (36)   NULL,
    [ltf_club_id]               VARCHAR (36)   NULL,
    [ltf_membership_id]         NVARCHAR (25)  NULL,
    [ltf_udw_id]                NVARCHAR (255) NULL,
    [modified_by]               VARCHAR (36)   NULL,
    [modified_on_behalf_by]     VARCHAR (36)   NULL,
    [opportunity_id]            VARCHAR (36)   NULL,
    [owner_id]                  VARCHAR (36)   NULL,
    [owning_business_unit]      VARCHAR (36)   NULL,
    [owning_team]               VARCHAR (36)   NULL,
    [owning_user]               VARCHAR (36)   NULL,
    [price_level_id]            VARCHAR (36)   NULL,
    [process_id]                VARCHAR (36)   NULL,
    [sales_order_id]            VARCHAR (36)   NULL,
    [stage_id]                  VARCHAR (36)   NULL,
    [transaction_currency_id]   VARCHAR (36)   NULL,
    [dv_load_date_time]         DATETIME       NOT NULL,
    [dv_r_load_source_id]       BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL,
    [dv_hash]                   CHAR (32)      NOT NULL,
    [dv_batch_id]               BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([l_crmcloudsync_invoice_id]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_crmcloudsync_invoice_delete]([dv_batch_id] ASC);

