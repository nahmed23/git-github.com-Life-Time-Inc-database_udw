CREATE TABLE [dbo].[idk_fact_magento_tran_item] (
    [idk_fact_magento_tran_item_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [unique_key]                    VARCHAR (33) NULL,
    [udw_inserted_datetime]         DATETIME     NULL,
    [allocated_datetime]            DATETIME     NULL,
    [canceled_datetime]             DATETIME     NULL,
    [dv_batch_id]                   BIGINT       NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

