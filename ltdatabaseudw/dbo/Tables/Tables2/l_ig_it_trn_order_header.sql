CREATE TABLE [dbo].[l_ig_it_trn_order_header] (
    [l_ig_it_trn_order_header_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)    NOT NULL,
    [bus_day_id]                  INT          NULL,
    [cashier_emp_id]              INT          NULL,
    [check_type_id]               INT          NULL,
    [close_term_id]               INT          NULL,
    [meal_period_id]              INT          NULL,
    [open_term_id]                INT          NULL,
    [order_hdr_id]                INT          NULL,
    [profit_center_id]            INT          NULL,
    [server_emp_id]               INT          NULL,
    [tender_id]                   INT          NULL,
    [void_reason_id]              INT          NULL,
    [dv_load_date_time]           DATETIME     NOT NULL,
    [dv_batch_id]                 BIGINT       NOT NULL,
    [dv_r_load_source_id]         BIGINT       NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL,
    [dv_hash]                     CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_ig_it_trn_order_header]
    ON [dbo].[l_ig_it_trn_order_header]([bk_hash] ASC, [l_ig_it_trn_order_header_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ig_it_trn_order_header]([dv_batch_id] ASC);

