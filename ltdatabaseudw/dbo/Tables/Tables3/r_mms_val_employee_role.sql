CREATE TABLE [dbo].[r_mms_val_employee_role] (
    [r_mms_val_employee_role_id] BIGINT       NOT NULL,
    [bk_hash]                    CHAR (32)    NOT NULL,
    [val_employee_role_id]       INT          NULL,
    [ltu_position_id]            INT          NULL,
    [description]                VARCHAR (50) NULL,
    [sort_order]                 INT          NULL,
    [department_id]              INT          NULL,
    [commissionable_flag]        BIT          NULL,
    [inserted_date_time]         DATETIME     NULL,
    [updated_date_time]          DATETIME     NULL,
    [hr_job_code]                VARCHAR (8)  NULL,
    [company_insider_type]       VARCHAR (50) NULL,
    [dv_load_date_time]          DATETIME     NOT NULL,
    [dv_load_end_date_time]      DATETIME     NOT NULL,
    [dv_batch_id]                BIGINT       NOT NULL,
    [dv_r_load_source_id]        BIGINT       NOT NULL,
    [dv_inserted_date_time]      DATETIME     NOT NULL,
    [dv_insert_user]             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]       DATETIME     NULL,
    [dv_update_user]             VARCHAR (50) NULL,
    [dv_hash]                    CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

