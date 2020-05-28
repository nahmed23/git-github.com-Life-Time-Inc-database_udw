CREATE TABLE [dbo].[s_exerp_subscription_sale] (
    [s_exerp_subscription_sale_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)       NOT NULL,
    [subscription_sale_id]         INT             NULL,
    [type]                         VARCHAR (4000)  NULL,
    [sale_datetime]                DATETIME        NULL,
    [start_date]                   DATETIME        NULL,
    [end_date]                     DATETIME        NULL,
    [jf_normal_price]              DECIMAL (26, 6) NULL,
    [jf_discount]                  DECIMAL (26, 6) NULL,
    [jf_price]                     DECIMAL (26, 6) NULL,
    [jf_sponsored]                 DECIMAL (26, 6) NULL,
    [jf_member]                    DECIMAL (26, 6) NULL,
    [prorata_period_normal_price]  DECIMAL (26, 6) NULL,
    [prorata_period_discount]      DECIMAL (26, 6) NULL,
    [prorata_period_price]         DECIMAL (26, 6) NULL,
    [prorata_period_sponsored]     DECIMAL (26, 6) NULL,
    [prorata_period_member]        DECIMAL (26, 6) NULL,
    [init_period_normal_price]     DECIMAL (26, 6) NULL,
    [init_period_discount]         DECIMAL (26, 6) NULL,
    [init_period_price]            DECIMAL (26, 6) NULL,
    [init_period_sponsored]        DECIMAL (26, 6) NULL,
    [init_period_member]           DECIMAL (26, 6) NULL,
    [admin_fee_normal_price]       DECIMAL (26, 6) NULL,
    [admin_fee_discount]           DECIMAL (26, 6) NULL,
    [admin_fee_price]              DECIMAL (26, 6) NULL,
    [admin_fee_sponsored]          DECIMAL (26, 6) NULL,
    [admin_fee_member]             DECIMAL (26, 6) NULL,
    [binding_days]                 INT             NULL,
    [init_contract_value]          DECIMAL (26, 6) NULL,
    [state]                        VARCHAR (4000)  NULL,
    [ets]                          BIGINT          NULL,
    [dummy_modified_date_time]     DATETIME        NULL,
    [dv_load_date_time]            DATETIME        NOT NULL,
    [dv_r_load_source_id]          BIGINT          NOT NULL,
    [dv_inserted_date_time]        DATETIME        NOT NULL,
    [dv_insert_user]               VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]         DATETIME        NULL,
    [dv_update_user]               VARCHAR (50)    NULL,
    [dv_hash]                      CHAR (32)       NOT NULL,
    [dv_deleted]                   BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                  BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exerp_subscription_sale]([dv_batch_id] ASC);

