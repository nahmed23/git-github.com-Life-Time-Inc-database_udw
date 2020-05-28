CREATE TABLE [dbo].[s_ig_it_trn_order_tender] (
    [s_ig_it_trn_order_tender_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [adtl_info]                   NVARCHAR (32)   NULL,
    [auth_acct_no]                NVARCHAR (32)   NULL,
    [breakage_amt]                DECIMAL (26, 6) NULL,
    [change_amt]                  DECIMAL (26, 6) NULL,
    [charges_to_date_amt]         DECIMAL (26, 6) NULL,
    [curr_dec_places]             SMALLINT        NULL,
    [customer_name]               NVARCHAR (32)   NULL,
    [exchange_rate]               DECIMAL (26, 6) NULL,
    [order_hdr_id]                INT             NULL,
    [pms_post_flag]               BIT             NULL,
    [post_system_1_flag]          BIT             NULL,
    [post_system_2_flag]          BIT             NULL,
    [post_system_3_flag]          BIT             NULL,
    [post_system_4_flag]          BIT             NULL,
    [post_system_5_flag]          BIT             NULL,
    [post_system_6_flag]          BIT             NULL,
    [post_system_7_flag]          BIT             NULL,
    [post_system_8_flag]          BIT             NULL,
    [pro_rata_discount_amt]       DECIMAL (26, 6) NULL,
    [pro_rata_grat_amt]           DECIMAL (26, 6) NULL,
    [pro_rata_sales_amt_gross]    DECIMAL (26, 6) NULL,
    [pro_rata_svc_chg_amt]        DECIMAL (26, 6) NULL,
    [pro_rata_tax_amt]            DECIMAL (26, 6) NULL,
    [received_curr_amt]           DECIMAL (26, 6) NULL,
    [remaining_balance_amt]       DECIMAL (26, 6) NULL,
    [sales_tippable_flag]         BIT             NULL,
    [sub_tender_qty]              INT             NULL,
    [tax_removed_code]            SMALLINT        NULL,
    [tender_amt]                  DECIMAL (26, 6) NULL,
    [tender_seq]                  SMALLINT        NULL,
    [tip_amt]                     DECIMAL (26, 6) NULL,
    [jan_one]                     DATETIME        NULL,
    [dv_load_date_time]           DATETIME        NOT NULL,
    [dv_r_load_source_id]         BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL,
    [dv_hash]                     CHAR (32)       NOT NULL,
    [dv_batch_id]                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_it_trn_order_tender]
    ON [dbo].[s_ig_it_trn_order_tender]([bk_hash] ASC, [s_ig_it_trn_order_tender_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_it_trn_order_tender]([dv_batch_id] ASC);

