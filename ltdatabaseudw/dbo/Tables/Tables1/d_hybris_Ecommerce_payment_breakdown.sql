﻿CREATE TABLE [dbo].[d_hybris_Ecommerce_payment_breakdown] (
    [d_hybris_Ecommerce_payment_breakdown_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [fact_hybris_payment_breakdown_key]       CHAR (32)       NULL,
    [order_number]                            VARCHAR (255)   NULL,
    [transaction_date]                        VARCHAR (50)    NULL,
    [oe_num]                                  INT             NULL,
    [club_id]                                 INT             NULL,
    [cost_center]                             VARCHAR (50)    NULL,
    [deposit]                                 VARCHAR (10)    NULL,
    [line_company]                            VARCHAR (100)   NULL,
    [revenue_category]                        VARCHAR (100)   NULL,
    [shipping_amount]                         FLOAT (53)      NULL,
    [spend_category]                          VARCHAR (100)   NULL,
    [tender_type_id]                          NVARCHAR (255)  NULL,
    [transaction_amount]                      DECIMAL (26, 6) NULL,
    [transaction_line_amount]                 DECIMAL (26, 6) NULL,
    [transaction_line_category_id]            NVARCHAR (255)  NULL,
    [transaction_line_memo]                   NVARCHAR (255)  NULL,
    [transaction_line_tax_amount]             DECIMAL (26, 6) NULL,
    [withdrawal]                              VARCHAR (10)    NULL,
    [workday_region]                          VARCHAR (4)     NULL,
    [p_hybris_Ecommerce_payment_breakdown_id] BIGINT          NOT NULL,
    [deleted_flag]                            INT             NULL,
    [dv_load_date_time]                       DATETIME        NULL,
    [dv_load_end_date_time]                   DATETIME        NULL,
    [dv_batch_id]                             BIGINT          NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_hybris_Ecommerce_payment_breakdown]([dv_batch_id] ASC);

