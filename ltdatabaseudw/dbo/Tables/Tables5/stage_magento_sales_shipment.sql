CREATE TABLE [dbo].[stage_magento_sales_shipment] (
    [stage_magento_sales_shipment_id] BIGINT          NOT NULL,
    [entity_id]                       INT             NULL,
    [store_id]                        INT             NULL,
    [total_weight]                    DECIMAL (12, 4) NULL,
    [total_qty]                       DECIMAL (12, 4) NULL,
    [email_sent]                      INT             NULL,
    [send_email]                      INT             NULL,
    [order_id]                        INT             NULL,
    [customer_id]                     INT             NULL,
    [shipping_address_id]             INT             NULL,
    [billing_address_id]              INT             NULL,
    [shipment_status]                 INT             NULL,
    [increment_id]                    VARCHAR (50)    NULL,
    [created_at]                      DATETIME        NULL,
    [updated_at]                      DATETIME        NULL,
    [packages]                        VARCHAR (8000)  NULL,
    [customer_note]                   VARCHAR (8000)  NULL,
    [customer_note_notify]            INT             NULL,
    [m1_shipment_id]                  INT             NULL,
    [dv_batch_id]                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

