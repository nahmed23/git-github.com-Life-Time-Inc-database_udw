CREATE TABLE [dbo].[l_hybris_orders] (
    [l_hybris_orders_id]            BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [type_pk_string]                BIGINT         NULL,
    [owner_pk_string]               BIGINT         NULL,
    [orders_pk]                     BIGINT         NULL,
    [p_currency]                    BIGINT         NULL,
    [p_delivery_address]            BIGINT         NULL,
    [p_delivery_mode]               BIGINT         NULL,
    [p_delivery_status]             BIGINT         NULL,
    [p_payment_address]             BIGINT         NULL,
    [p_payment_info]                BIGINT         NULL,
    [p_payment_mode]                BIGINT         NULL,
    [p_payment_status]              BIGINT         NULL,
    [p_status]                      BIGINT         NULL,
    [p_export_status]               BIGINT         NULL,
    [p_user]                        BIGINT         NULL,
    [p_europe_1_price_factory_udg]  BIGINT         NULL,
    [p_europe_1_price_factory_upg]  BIGINT         NULL,
    [p_europe_1_price_factory_utg]  BIGINT         NULL,
    [p_previous_delivery_mode]      BIGINT         NULL,
    [p_site]                        BIGINT         NULL,
    [p_store]                       BIGINT         NULL,
    [p_billing_time]                BIGINT         NULL,
    [p_parent]                      BIGINT         NULL,
    [p_original_version]            BIGINT         NULL,
    [p_sales_application]           BIGINT         NULL,
    [p_language]                    BIGINT         NULL,
    [p_placed_by]                   BIGINT         NULL,
    [p_fulfilment_status]           BIGINT         NULL,
    [p_fraud_prevention_session_id] NVARCHAR (255) NULL,
    [p_commission_employee_id]      NVARCHAR (255) NULL,
    [dv_load_date_time]             DATETIME       NOT NULL,
    [dv_r_load_source_id]           BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL,
    [dv_hash]                       CHAR (32)      NOT NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_orders]
    ON [dbo].[l_hybris_orders]([bk_hash] ASC, [l_hybris_orders_id] ASC);

