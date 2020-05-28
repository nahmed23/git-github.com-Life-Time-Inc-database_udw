CREATE TABLE [dbo].[p_hybris_Ecommerce_payment_breakdown] (
    [p_hybris_Ecommerce_payment_breakdown_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)     NOT NULL,
    [order_num]                               VARCHAR (255) NULL,
    [oe_num]                                  INT           NULL,
    [tran_date]                               VARCHAR (29)  NULL,
    [l_hybris_Ecommerce_payment_breakdown_id] BIGINT        NULL,
    [s_hybris_Ecommerce_payment_breakdown_id] BIGINT        NULL,
    [dv_load_date_time]                       DATETIME      NOT NULL,
    [dv_load_end_date_time]                   DATETIME      NOT NULL,
    [dv_greatest_satellite_date_time]         DATETIME      NULL,
    [dv_next_greatest_satellite_date_time]    DATETIME      NULL,
    [dv_first_in_key_series]                  INT           NULL,
    [dv_inserted_date_time]                   DATETIME      NOT NULL,
    [dv_insert_user]                          VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                    DATETIME      NULL,
    [dv_update_user]                          VARCHAR (50)  NULL,
    [dv_batch_id]                             BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_hybris_Ecommerce_payment_breakdown]([dv_batch_id] ASC);

