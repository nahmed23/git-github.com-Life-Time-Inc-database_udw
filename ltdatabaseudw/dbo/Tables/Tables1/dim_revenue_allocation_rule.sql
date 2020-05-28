CREATE TABLE [dbo].[dim_revenue_allocation_rule] (
    [accumulated_ratio]                                          DECIMAL (11, 10) NULL,
    [club_id]                                                    INT              NULL,
    [earliest_transaction_dim_date_key]                          CHAR (8)         NULL,
    [effective_date]                                             DATETIME         NULL,
    [expiration_date]                                            DATETIME         NULL,
    [latest_transaction_dim_date_key]                            CHAR (8)         NULL,
    [one_off_rule_flag]                                          CHAR (1)         NULL,
    [ratio]                                                      DECIMAL (11, 10) NULL,
    [revenue_allocation_rule_name]                               VARCHAR (50)     NULL,
    [revenue_allocation_rule_set]                                VARCHAR (60)     NULL,
    [revenue_from_late_transaction_flag]                         CHAR (1)         NULL,
    [revenue_posting_month_ending_dim_date_key]                  CHAR (8)         NULL,
    [revenue_posting_month_four_digit_year_dash_two_digit_month] CHAR (10)        NULL,
    [revenue_posting_month_starting_dim_date_key]                CHAR (8)         NULL,
    [dv_load_date_time]                                          DATETIME         NULL,
    [dv_load_end_date_time]                                      DATETIME         NULL,
    [dv_inserted_date_time]                                      DATETIME         NOT NULL,
    [dv_insert_user]                                             VARCHAR (50)     NOT NULL,
    [dv_updated_date_time]                                       DATETIME         NULL,
    [dv_update_user]                                             VARCHAR (50)     NULL,
    [dim_revenue_allocation_rule_id]                             INT              NULL,
    [dv_batch_id]                                                INT              NULL,
    [dim_club_key]                                               VARCHAR (32)     NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);

