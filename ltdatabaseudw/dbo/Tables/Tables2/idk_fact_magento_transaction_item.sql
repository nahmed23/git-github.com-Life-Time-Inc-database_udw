CREATE TABLE [dbo].[idk_fact_magento_transaction_item] (
    [idk_fact_magento_transaction_item_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [fact_magento_invoice_item_key]        VARCHAR (32) NULL,
    [fact_magento_refund_item_key]         VARCHAR (32) NULL,
    [udw_inserted_date_time]               DATETIME     NULL,
    [udw_inserted_dim_date_key]            VARCHAR (8)  NULL,
    [dv_batch_id]                          BIGINT       NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

