CREATE TABLE [dbo].[s_ltfmart_employee_goals] (
    [s_ltfmart_employee_goals_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [employee_goals_id]           INT             NULL,
    [goal_date]                   DATETIME        NULL,
    [appointment_show_goal]       INT             NULL,
    [membership_goal]             INT             NULL,
    [vip_referral_goal]           INT             NULL,
    [hours_worked]                INT             NULL,
    [unit_quota]                  INT             NULL,
    [unit_quota_override]         INT             NULL,
    [ndt_quota]                   DECIMAL (26, 6) NULL,
    [has_goal]                    BIT             NULL,
    [is_as_dh]                    BIT             NULL,
    [is_dept_head]                BIT             NULL,
    [is_new_hire]                 BIT             NULL,
    [is_part_time]                BIT             NULL,
    [override_unit_quota]         BIT             NULL,
    [sales_comp_exception]        BIT             NULL,
    [override_user_role]          BIT             NULL,
    [inactive_date]               DATETIME        NULL,
    [create_date_time]            DATETIME        NULL,
    [modify_date_time]            DATETIME        NULL,
    [row_version]                 BINARY (8)      NULL,
    [dv_load_date_time]           DATETIME        NOT NULL,
    [dv_r_load_source_id]         BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL,
    [dv_hash]                     CHAR (32)       NOT NULL,
    [dv_batch_id]                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_ltfmart_employee_goals]
    ON [dbo].[s_ltfmart_employee_goals]([bk_hash] ASC, [s_ltfmart_employee_goals_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ltfmart_employee_goals]([dv_batch_id] ASC);

