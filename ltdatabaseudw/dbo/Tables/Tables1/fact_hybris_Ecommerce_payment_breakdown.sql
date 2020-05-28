CREATE TABLE [dbo].[fact_hybris_Ecommerce_payment_breakdown] (
    [fact_hybris_Ecommerce_payment_breakdown_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [fact_hybris_Ecommerce_payment_breakdown_key] CHAR (32)       NULL,
    [company_id]                                  VARCHAR (50)    NULL,
    [cost_center_id]                              VARCHAR (100)   NULL,
    [currency_id]                                 VARCHAR (50)    NULL,
    [posted_date]                                 DATETIME        NULL,
    [region_id]                                   VARCHAR (100)   NULL,
    [tender_type_id]                              NVARCHAR (50)   NULL,
    [transaction_amount]                          DECIMAL (26, 2) NULL,
    [transaction_date]                            DATETIME        NULL,
    [transaction_id]                              NVARCHAR (500)  NULL,
    [transaction_line_amount]                     DECIMAL (26, 2) NULL,
    [transaction_line_category_id]                VARCHAR (50)    NULL,
    [transaction_memo]                            VARCHAR (200)   NULL,
    [dv_load_date_time]                           DATETIME        NULL,
    [dv_load_end_date_time]                       DATETIME        NULL,
    [dv_batch_id]                                 BIGINT          NOT NULL,
    [dv_inserted_date_time]                       DATETIME        NOT NULL,
    [dv_insert_user]                              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                        DATETIME        NULL,
    [dv_update_user]                              VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_hybris_Ecommerce_payment_breakdown_key]));

