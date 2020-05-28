CREATE TABLE [dbo].[dim_employee_role] (
    [dim_employee_role_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [val_employee_role_id]  INT          NULL,
    [commissionable_flag]   CHAR (1)     NULL,
    [dim_employee_role_key] CHAR (32)    NULL,
    [mms_department_name]   VARCHAR (50) NULL,
    [role_name]             VARCHAR (50) NULL,
    [dv_load_date_time]     DATETIME     NULL,
    [dv_load_end_date_time] DATETIME     NULL,
    [dv_batch_id]           BIGINT       NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_employee_role_key]));

