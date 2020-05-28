CREATE TABLE [dbo].[s_olo_order_detail] (
    [s_olo_order_detail_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [message_id]            VARCHAR (255)   NULL,
    [event_type]            VARCHAR (255)   NULL,
    [time_cancelled]        VARCHAR (29)    NULL,
    [cancel_reason]         VARCHAR (255)   NULL,
    [external_reference]    VARCHAR (255)   NULL,
    [store_number]          VARCHAR (255)   NULL,
    [time_placed]           VARCHAR (29)    NULL,
    [time_wanted]           VARCHAR (29)    NULL,
    [time_ready]            VARCHAR (29)    NULL,
    [sub_total]             DECIMAL (26, 2) NULL,
    [sales_tax]             DECIMAL (26, 2) NULL,
    [tip]                   DECIMAL (26, 2) NULL,
    [delivery]              DECIMAL (26, 2) NULL,
    [discount]              DECIMAL (26, 2) NULL,
    [total]                 DECIMAL (26, 2) NULL,
    [customer_delivery]     DECIMAL (26, 2) NULL,
    [payment_type]          VARCHAR (255)   NULL,
    [payment_description]   VARCHAR (255)   NULL,
    [amount]                DECIMAL (26, 2) NULL,
    [time_adjusted]         VARCHAR (29)    NULL,
    [adjustment_amount]     DECIMAL (26, 2) NULL,
    [adjustment_type]       VARCHAR (255)   NULL,
    [adjustment_reason]     VARCHAR (255)   NULL,
    [time_closed]           VARCHAR (29)    NULL,
    [jan_one]               DATETIME        NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL,
    [dv_deleted]            BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_olo_order_detail]([dv_batch_id] ASC);

