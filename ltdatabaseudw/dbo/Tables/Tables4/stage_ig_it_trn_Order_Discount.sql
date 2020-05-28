CREATE TABLE [dbo].[stage_ig_it_trn_Order_Discount] (
    [stage_ig_it_trn_Order_Discount_id] BIGINT   NOT NULL,
    [check_seq]                         SMALLINT NULL,
    [discount_amt]                      MONEY    NULL,
    [discoup_id]                        INT      NULL,
    [emp_id]                            INT      NULL,
    [num_items]                         SMALLINT NULL,
    [order_hdr_id]                      INT      NULL,
    [tax_amt_incl_disc]                 MONEY    NULL,
    [taxable_disc_flag]                 BIT      NULL,
    [jan_one]                           DATETIME NULL,
    [dv_batch_id]                       BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

