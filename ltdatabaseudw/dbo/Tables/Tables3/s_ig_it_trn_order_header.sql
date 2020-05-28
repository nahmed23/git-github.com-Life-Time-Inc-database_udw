CREATE TABLE [dbo].[s_ig_it_trn_order_header] (
    [s_ig_it_trn_order_header_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [assoc_check_no]              INT             NULL,
    [check_no]                    INT             NULL,
    [check_status]                SMALLINT        NULL,
    [close_dttime]                DATETIME        NULL,
    [discount_amt]                DECIMAL (26, 6) NULL,
    [drawer_no]                   SMALLINT        NULL,
    [grat_amt]                    DECIMAL (26, 6) NULL,
    [num_covers]                  SMALLINT        NULL,
    [open_dttime]                 DATETIME        NULL,
    [order_hdr_id]                INT             NULL,
    [order_process_dttime]        DATETIME        NULL,
    [pretender_flag]              BIT             NULL,
    [print_count]                 SMALLINT        NULL,
    [refund_flag]                 BIT             NULL,
    [sales_amt_gross]             DECIMAL (26, 6) NULL,
    [sales_tippable_flag]         BIT             NULL,
    [service_charge_amt]          DECIMAL (26, 6) NULL,
    [table_alpha_no]              NVARCHAR (30)   NULL,
    [table_no]                    SMALLINT        NULL,
    [tax_amt]                     DECIMAL (26, 6) NULL,
    [tax_removd_flag]             BIT             NULL,
    [tender_amt_gross]            DECIMAL (26, 6) NULL,
    [tip_amt]                     DECIMAL (26, 6) NULL,
    [tran_data_tag_text]          NVARCHAR (30)   NULL,
    [jan_one]                     DATETIME        NULL,
    [dv_load_date_time]           DATETIME        NOT NULL,
    [dv_batch_id]                 BIGINT          NOT NULL,
    [dv_r_load_source_id]         BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL,
    [dv_hash]                     CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ig_it_trn_order_header]
    ON [dbo].[s_ig_it_trn_order_header]([bk_hash] ASC, [dv_load_date_time] ASC, [s_ig_it_trn_order_header_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ig_it_trn_order_header]([dv_batch_id] ASC);

