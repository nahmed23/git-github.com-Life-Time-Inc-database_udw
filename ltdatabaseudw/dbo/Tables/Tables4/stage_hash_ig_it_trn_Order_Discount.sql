CREATE TABLE [dbo].[stage_hash_ig_it_trn_Order_Discount] (
    [stage_hash_ig_it_trn_Order_Discount_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)    NOT NULL,
    [check_seq]                              SMALLINT     NULL,
    [discount_amt]                           MONEY        NULL,
    [discoup_id]                             INT          NULL,
    [emp_id]                                 INT          NULL,
    [num_items]                              SMALLINT     NULL,
    [order_hdr_id]                           INT          NULL,
    [tax_amt_incl_disc]                      MONEY        NULL,
    [taxable_disc_flag]                      BIT          NULL,
    [jan_one]                                DATETIME     NULL,
    [dv_load_date_time]                      DATETIME     NOT NULL,
    [dv_inserted_date_time]                  DATETIME     NOT NULL,
    [dv_insert_user]                         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                   DATETIME     NULL,
    [dv_update_user]                         VARCHAR (50) NULL,
    [dv_batch_id]                            BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

