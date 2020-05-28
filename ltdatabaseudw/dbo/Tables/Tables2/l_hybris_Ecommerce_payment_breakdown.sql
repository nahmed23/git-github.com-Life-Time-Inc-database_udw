CREATE TABLE [dbo].[l_hybris_Ecommerce_payment_breakdown] (
    [l_hybris_Ecommerce_payment_breakdown_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)     NOT NULL,
    [club_id]                                 INT           NULL,
    [offering_id]                             VARCHAR (255) NULL,
    [order_num]                               VARCHAR (255) NULL,
    [oe_num]                                  INT           NULL,
    [product_id]                              VARCHAR (255) NULL,
    [mms_product_id]                          INT           NULL,
    [tran_date]                               VARCHAR (29)  NULL,
    [mms_transaction_id]                      VARCHAR (255) NULL,
    [mms_package_id]                          INT           NULL,
    [member_id]                               VARCHAR (255) NULL,
    [dv_load_date_time]                       DATETIME      NOT NULL,
    [dv_r_load_source_id]                     BIGINT        NOT NULL,
    [dv_inserted_date_time]                   DATETIME      NOT NULL,
    [dv_insert_user]                          VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                    DATETIME      NULL,
    [dv_update_user]                          VARCHAR (50)  NULL,
    [dv_hash]                                 CHAR (32)     NOT NULL,
    [dv_batch_id]                             BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_hybris_Ecommerce_payment_breakdown]([dv_batch_id] ASC);

