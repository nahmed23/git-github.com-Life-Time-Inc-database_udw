CREATE TABLE [dbo].[h_hybris_Ecommerce_payment_breakdown] (
    [h_hybris_Ecommerce_payment_breakdown_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)     NOT NULL,
    [order_num]                               VARCHAR (255) NULL,
    [oe_num]                                  INT           NULL,
    [tran_date]                               VARCHAR (29)  NULL,
    [dv_load_date_time]                       DATETIME      NOT NULL,
    [dv_r_load_source_id]                     BIGINT        NOT NULL,
    [dv_inserted_date_time]                   DATETIME      NOT NULL,
    [dv_insert_user]                          VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                    DATETIME      NULL,
    [dv_update_user]                          VARCHAR (50)  NULL,
    [dv_deleted]                              BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                             BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

