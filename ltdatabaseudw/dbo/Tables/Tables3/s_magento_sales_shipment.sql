CREATE TABLE [dbo].[s_magento_sales_shipment] (
    [s_magento_sales_shipment_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [entity_id]                   INT             NULL,
    [total_weight]                DECIMAL (12, 4) NULL,
    [total_qty]                   DECIMAL (12, 4) NULL,
    [email_sent]                  INT             NULL,
    [send_email]                  INT             NULL,
    [shipment_status]             INT             NULL,
    [increment_id]                VARCHAR (50)    NULL,
    [created_at]                  DATETIME        NULL,
    [updated_at]                  DATETIME        NULL,
    [packages]                    VARCHAR (8000)  NULL,
    [customer_note]               VARCHAR (8000)  NULL,
    [customer_note_notify]        INT             NULL,
    [dv_load_date_time]           DATETIME        NOT NULL,
    [dv_r_load_source_id]         BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL,
    [dv_hash]                     CHAR (32)       NOT NULL,
    [dv_deleted]                  BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                 BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

