CREATE TABLE [dbo].[h_hybris_seven_day_orders] (
    [h_hybris_seven_day_orders_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [order_code]                   NVARCHAR (255) NULL,
    [order_entry_entry_number]     INT            NULL,
    [dv_load_date_time]            DATETIME       NOT NULL,
    [dv_r_load_source_id]          BIGINT         NOT NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL,
    [dv_deleted]                   BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

