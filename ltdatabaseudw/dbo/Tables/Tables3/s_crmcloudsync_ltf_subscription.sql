CREATE TABLE [dbo].[s_crmcloudsync_ltf_subscription] (
    [s_crmcloudsync_ltf_subscription_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [created_by_name]                     NVARCHAR (200)  NULL,
    [created_by_yomi_name]                NVARCHAR (200)  NULL,
    [created_on]                          DATETIME        NULL,
    [created_on_behalf_by_name]           NVARCHAR (200)  NULL,
    [created_on_behalf_by_yomi_name]      NVARCHAR (200)  NULL,
    [exchange_rate]                       DECIMAL (28)    NULL,
    [import_sequence_number]              INT             NULL,
    [ltf_account_id_name]                 NVARCHAR (160)  NULL,
    [ltf_account_id_yomi_name]            NVARCHAR (160)  NULL,
    [ltf_activation_date]                 DATETIME        NULL,
    [ltf_cancellation_date]               DATETIME        NULL,
    [ltf_club_id_name]                    NVARCHAR (100)  NULL,
    [ltf_cost]                            DECIMAL (26, 6) NULL,
    [ltf_cost_base]                       DECIMAL (26, 6) NULL,
    [ltf_customer_company_code]           NVARCHAR (100)  NULL,
    [ltf_product_id_name]                 NVARCHAR (100)  NULL,
    [ltf_referring_contact_id_name]       NVARCHAR (160)  NULL,
    [ltf_referring_contact_id_yomi_name]  NVARCHAR (160)  NULL,
    [ltf_subscription_id]                 VARCHAR (36)    NULL,
    [ltf_subscription_number]             NVARCHAR (100)  NULL,
    [ltf_termination_date]                DATETIME        NULL,
    [ltf_termination_reason]              NVARCHAR (100)  NULL,
    [modified_by_name]                    NVARCHAR (200)  NULL,
    [modified_by_yomi_name]               NVARCHAR (200)  NULL,
    [modified_on]                         DATETIME        NULL,
    [modified_on_behalf_by_name]          NVARCHAR (200)  NULL,
    [modified_on_behalf_by_yomi_name]     NVARCHAR (200)  NULL,
    [overridden_created_on]               DATETIME        NULL,
    [owner_id_name]                       NVARCHAR (200)  NULL,
    [owner_id_type]                       NVARCHAR (64)   NULL,
    [owner_id_yomi_name]                  NVARCHAR (200)  NULL,
    [state_code]                          INT             NULL,
    [state_code_name]                     NVARCHAR (255)  NULL,
    [status_code]                         INT             NULL,
    [status_code_name]                    NVARCHAR (255)  NULL,
    [time_zone_rule_version_number]       INT             NULL,
    [transaction_currency_id_name]        NVARCHAR (100)  NULL,
    [utc_conversion_time_zone_code]       INT             NULL,
    [version_number]                      BIGINT          NULL,
    [inserted_date_time]                  DATETIME        NULL,
    [insert_user]                         VARCHAR (100)   NULL,
    [updated_date_time]                   DATETIME        NULL,
    [update_user]                         VARCHAR (50)    NULL,
    [ltf_club_portfolio_staffing_id_name] NVARCHAR (100)  NULL,
    [ltf_lt_health_reactivation_date]     DATETIME        NULL,
    [ltf_monthly_cost_of_membership]      DECIMAL (26, 6) NULL,
    [ltf_attrition_exclusion]             BIT             NULL,
    [ltf_attrition_exclusion_name]        NVARCHAR (255)  NULL,
    [ltf_account_household_name]          NVARCHAR (160)  NULL,
    [ltf_account_household_yomi_id_name]  NVARCHAR (160)  NULL,
    [ltf_monthly_cost_of_membership_base] DECIMAL (26, 6) NULL,
    [ltf_revenue_unit]                    BIT             NULL,
    [ltf_revenue_unit_name]               NVARCHAR (255)  NULL,
    [dv_load_date_time]                   DATETIME        NOT NULL,
    [dv_r_load_source_id]                 BIGINT          NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL,
    [dv_hash]                             CHAR (32)       NOT NULL,
    [dv_batch_id]                         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_crmcloudsync_ltf_subscription]
    ON [dbo].[s_crmcloudsync_ltf_subscription]([bk_hash] ASC, [s_crmcloudsync_ltf_subscription_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_crmcloudsync_ltf_subscription]([dv_batch_id] ASC);

