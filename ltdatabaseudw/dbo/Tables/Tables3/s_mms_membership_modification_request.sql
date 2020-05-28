CREATE TABLE [dbo].[s_mms_membership_modification_request] (
    [s_mms_membership_modification_request_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)       NOT NULL,
    [membership_modification_request_id]       INT             NULL,
    [request_date_time]                        DATETIME        NULL,
    [utc_request_date_time]                    DATETIME        NULL,
    [request_date_time_zone]                   VARCHAR (4)     NULL,
    [effective_date]                           DATETIME        NULL,
    [inserted_date_time]                       DATETIME        NULL,
    [updated_date_time]                        DATETIME        NULL,
    [status_changed_date_time]                 DATETIME        NULL,
    [last_eft_month]                           VARCHAR (9)     NULL,
    [future_membership_upgrade_flag]           BIT             NULL,
    [first_months_dues]                        DECIMAL (26, 6) NULL,
    [total_monthly_amount]                     DECIMAL (26, 6) NULL,
    [membership_upgrade_month_year]            DATETIME        NULL,
    [agreement_price]                          DECIMAL (26, 6) NULL,
    [waive_service_fee_flag]                   BIT             NULL,
    [full_access_date_extension_flag]          BIT             NULL,
    [new_members]                              VARCHAR (200)   NULL,
    [add_on_fee]                               DECIMAL (26, 6) NULL,
    [service_fee]                              DECIMAL (26, 6) NULL,
    [diamond_fee]                              DECIMAL (26, 6) NULL,
    [pro_rated_dues]                           DECIMAL (26, 6) NULL,
    [deactivated_members]                      VARCHAR (200)   NULL,
    [juniors_assessed]                         INT             NULL,
    [member_freeze_flag]                       BIT             NULL,
    [dv_load_date_time]                        DATETIME        NOT NULL,
    [dv_r_load_source_id]                      BIGINT          NOT NULL,
    [dv_inserted_date_time]                    DATETIME        NOT NULL,
    [dv_insert_user]                           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                     DATETIME        NULL,
    [dv_update_user]                           VARCHAR (50)    NULL,
    [dv_hash]                                  CHAR (32)       NOT NULL,
    [dv_batch_id]                              BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_membership_modification_request]([dv_batch_id] ASC);

