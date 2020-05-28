CREATE TABLE [dbo].[d_ig_it_trn_order_tender] (
    [d_ig_it_trn_order_tender_id]              BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)       NOT NULL,
    [fact_cafe_payment_key]                    CHAR (32)       NULL,
    [order_hdr_id]                             INT             NULL,
    [tender_seq]                               INT             NULL,
    [change_amount]                            DECIMAL (26, 6) NULL,
    [charges_to_date_amount]                   DECIMAL (26, 6) NULL,
    [d_ig_it_trn_order_header_bk_hash]         CHAR (32)       NULL,
    [d_mms_pt_credit_card_transaction_bk_hash] CHAR (32)       NULL,
    [dim_cafe_payment_type_key]                CHAR (32)       NULL,
    [pro_rata_discount_amount]                 DECIMAL (26, 6) NULL,
    [pro_rata_gratuity_amount]                 DECIMAL (26, 6) NULL,
    [pro_rata_sales_amount_gross]              DECIMAL (26, 6) NULL,
    [pro_rata_service_charge_amount]           DECIMAL (26, 6) NULL,
    [pro_rata_tax_amount]                      DECIMAL (26, 6) NULL,
    [remaining_balance_amount]                 DECIMAL (26, 6) NULL,
    [tender_amount]                            DECIMAL (26, 6) NULL,
    [tender_id]                                INT             NULL,
    [tender_type_id]                           INT             NULL,
    [tip_amount]                               DECIMAL (26, 6) NULL,
    [p_ig_it_trn_order_tender_id]              BIGINT          NOT NULL,
    [deleted_flag]                             INT             NULL,
    [dv_load_date_time]                        DATETIME        NULL,
    [dv_load_end_date_time]                    DATETIME        NULL,
    [dv_batch_id]                              BIGINT          NOT NULL,
    [dv_inserted_date_time]                    DATETIME        NOT NULL,
    [dv_insert_user]                           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                     DATETIME        NULL,
    [dv_update_user]                           VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_ig_it_trn_order_tender]([dv_batch_id] ASC);

