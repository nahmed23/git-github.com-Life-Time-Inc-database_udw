CREATE TABLE [dbo].[s_ig_it_trn_order_item] (
    [s_ig_it_trn_order_item_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)       NOT NULL,
    [check_seq]                 SMALLINT        NULL,
    [discount_amt]              DECIMAL (26, 6) NULL,
    [item_qty]                  INT             NULL,
    [order_hdr_id]              INT             NULL,
    [sales_amt_gross]           DECIMAL (26, 6) NULL,
    [split_item_flag]           CHAR (1)        NULL,
    [tax_amt_incl_disc]         DECIMAL (26, 6) NULL,
    [tax_amt_incl_sales]        DECIMAL (26, 6) NULL,
    [tax_incl_flag]             CHAR (1)        NULL,
    [jan_one]                   DATETIME        NULL,
    [dv_load_date_time]         DATETIME        NOT NULL,
    [dv_r_load_source_id]       BIGINT          NOT NULL,
    [dv_inserted_date_time]     DATETIME        NOT NULL,
    [dv_insert_user]            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]      DATETIME        NULL,
    [dv_update_user]            VARCHAR (50)    NULL,
    [dv_hash]                   CHAR (32)       NOT NULL,
    [dv_batch_id]               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_it_trn_order_item]
    ON [dbo].[s_ig_it_trn_order_item]([bk_hash] ASC, [s_ig_it_trn_order_item_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_it_trn_order_item]([dv_batch_id] ASC);

