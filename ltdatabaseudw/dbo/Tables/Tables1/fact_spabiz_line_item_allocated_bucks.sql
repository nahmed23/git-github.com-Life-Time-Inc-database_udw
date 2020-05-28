CREATE TABLE [dbo].[fact_spabiz_line_item_allocated_bucks] (
    [fact_spabiz_line_item_allocated_bucks_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [allocated_bucks_payment_amount]           DECIMAL (26, 6) NULL,
    [dim_spabiz_segment_key]                   CHAR (32)       NULL,
    [fact_spabiz_ticket_item_key]              CHAR (32)       NULL,
    [fact_spabiz_ticket_key]                   CHAR (32)       NULL,
    [line_item_amount]                         DECIMAL (26, 6) NULL,
    [bk_hash]                                  CHAR (32)       NULL,
    [dv_load_date_time]                        DATETIME        NULL,
    [dv_load_end_date_time]                    DATETIME        NULL,
    [dv_batch_id]                              BIGINT          NULL,
    [dv_inserted_date_time]                    DATETIME        NOT NULL,
    [dv_insert_user]                           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                     DATETIME        NULL,
    [dv_update_user]                           VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

