CREATE TABLE [dbo].[fact_ltfmart_employee_goals] (
    [fact_ltfmart_employee_goals_id]   BIGINT       IDENTITY (1, 1) NOT NULL,
    [fact_ltfmart_employee_goals_key]  CHAR (32)    NULL,
    [employee_goals_id]                CHAR (32)    NULL,
    [fact_employee_goals_club_key]     CHAR (32)    NULL,
    [fact_employee_goals_employee_key] CHAR (32)    NULL,
    [goal_dollar_amount]               INT          NULL,
    [goal_first_of_month_date]         DATETIME     NULL,
    [goal_first_of_month_dim_date_key] CHAR (8)     NULL,
    [goal_name]                        VARCHAR (50) NULL,
    [goal_quantity]                    INT          NULL,
    [dv_load_date_time]                DATETIME     NULL,
    [dv_load_end_date_time]            DATETIME     NULL,
    [dv_batch_id]                      BIGINT       NULL,
    [dv_inserted_date_time]            DATETIME     NOT NULL,
    [dv_insert_user]                   VARCHAR (50) NOT NULL,
    [dv_updated_date_time]             DATETIME     NULL,
    [dv_update_user]                   VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_ltfmart_employee_goals_key]));

