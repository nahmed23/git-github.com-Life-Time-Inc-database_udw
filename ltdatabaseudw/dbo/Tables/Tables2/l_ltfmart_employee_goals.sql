CREATE TABLE [dbo].[l_ltfmart_employee_goals] (
    [l_ltfmart_employee_goals_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)    NOT NULL,
    [employee_goals_id]           INT          NULL,
    [employee_id]                 INT          NULL,
    [club_id]                     INT          NULL,
    [val_compensation_plan_id]    INT          NULL,
    [new_hire_type_id]            INT          NULL,
    [ma_level_id]                 INT          NULL,
    [sdh_type_id]                 INT          NULL,
    [create_emp_id]               INT          NULL,
    [modify_emp_id]               INT          NULL,
    [dv_load_date_time]           DATETIME     NOT NULL,
    [dv_r_load_source_id]         BIGINT       NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL,
    [dv_hash]                     CHAR (32)    NOT NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_ltfmart_employee_goals]
    ON [dbo].[l_ltfmart_employee_goals]([bk_hash] ASC, [l_ltfmart_employee_goals_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ltfmart_employee_goals]([dv_batch_id] ASC);

