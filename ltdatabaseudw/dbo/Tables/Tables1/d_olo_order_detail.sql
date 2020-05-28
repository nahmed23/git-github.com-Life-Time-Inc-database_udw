CREATE TABLE [dbo].[d_olo_order_detail] (
    [d_olo_order_detail_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)       NOT NULL,
    [fact_olo_order_detail_key] CHAR (32)       NULL,
    [adjustment_amount]         DECIMAL (26, 2) NULL,
    [event_type]                VARCHAR (100)   NULL,
    [order_id]                  VARCHAR (100)   NULL,
    [payment_description]       VARCHAR (255)   NULL,
    [payment_type]              VARCHAR (255)   NULL,
    [sale_amount]               DECIMAL (26, 2) NULL,
    [sale_total]                DECIMAL (26, 2) NULL,
    [store_number]              VARCHAR (100)   NULL,
    [time_adjusted]             DATE            NULL,
    [time_cancelled]            DATE            NULL,
    [time_placed]               DATE            NULL,
    [transaction_amount]        DECIMAL (26, 2) NULL,
    [transaction_date]          DATE            NULL,
    [p_olo_order_detail_id]     BIGINT          NOT NULL,
    [deleted_flag]              INT             NULL,
    [dv_load_date_time]         DATETIME        NULL,
    [dv_load_end_date_time]     DATETIME        NULL,
    [dv_batch_id]               BIGINT          NOT NULL,
    [dv_inserted_date_time]     DATETIME        NOT NULL,
    [dv_insert_user]            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]      DATETIME        NULL,
    [dv_update_user]            VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_olo_order_detail]([dv_batch_id] ASC);

