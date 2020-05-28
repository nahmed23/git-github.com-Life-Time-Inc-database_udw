CREATE TABLE [dbo].[s_mms_club] (
    [s_mms_club_id]                         BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)     NOT NULL,
    [club_id]                               INT           NULL,
    [domain_name_prefix]                    VARCHAR (10)  NULL,
    [club_name]                             VARCHAR (50)  NULL,
    [receipt_footer]                        VARCHAR (100) NULL,
    [display_ui_flag]                       BIT           NULL,
    [check_in_group_level]                  INT           NULL,
    [charge_to_account_flag]                BIT           NULL,
    [club_activation_date]                  DATETIME      NULL,
    [inserted_date_time]                    DATETIME      NULL,
    [crm_division_code]                     VARCHAR (15)  NULL,
    [assess_junior_member_dues_flag]        BIT           NULL,
    [sell_junior_member_dues_flag]          BIT           NULL,
    [club_code]                             VARCHAR (3)   NULL,
    [new_member_card_flag]                  BIT           NULL,
    [child_center_weekly_limit]             INT           NULL,
    [formal_club_name]                      VARCHAR (140) NULL,
    [kronos_forecast_map_path]              VARCHAR (15)  NULL,
    [club_deactivation_date]                DATETIME      NULL,
    [gl_cash_entry_account]                 VARCHAR (4)   NULL,
    [gl_receivables_entry_account]          VARCHAR (4)   NULL,
    [gl_cash_entry_cash_sub_account]        VARCHAR (11)  NULL,
    [gl_cash_entry_credit_card_sub_account] VARCHAR (11)  NULL,
    [gl_receivables_entry_sub_account]      VARCHAR (11)  NULL,
    [gl_cash_entry_company_name]            VARCHAR (15)  NULL,
    [gl_receivables_entry_company_name]     VARCHAR (15)  NULL,
    [marketing_map_region]                  VARCHAR (50)  NULL,
    [marketing_map_xml_state_name]          VARCHAR (50)  NULL,
    [marketing_club_level]                  VARCHAR (50)  NULL,
    [allow_multiple_currency_flag]          BIT           NULL,
    [workday_region]                        VARCHAR (4)   NULL,
    [allow_junior_check_in_flag]            BIT           NULL,
    [health_mms_club_identifier]            VARCHAR (50)  NULL,
    [max_junior_age]                        INT           NULL,
    [max_secondary_age]                     INT           NULL,
    [charge_next_month_date]                INT           NULL,
    [min_front_desk_checkin_age]            INT           NULL,
    [max_child_center_checkin_age]          INT           NULL,
    [updated_date_time]                     DATETIME      NULL,
    [dv_load_date_time]                     DATETIME      NOT NULL,
    [dv_batch_id]                           BIGINT        NOT NULL,
    [dv_r_load_source_id]                   BIGINT        NOT NULL,
    [dv_inserted_date_time]                 DATETIME      NOT NULL,
    [dv_insert_user]                        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                  DATETIME      NULL,
    [dv_update_user]                        VARCHAR (50)  NULL,
    [dv_hash]                               CHAR (32)     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_club]
    ON [dbo].[s_mms_club]([bk_hash] ASC, [s_mms_club_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_club]([dv_batch_id] ASC);

