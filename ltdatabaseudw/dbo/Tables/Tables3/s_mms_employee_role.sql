CREATE TABLE [dbo].[s_mms_employee_role] (
    [s_mms_employee_role_id]     BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)    NOT NULL,
    [employee_role_id]           INT          NULL,
    [inserted_date_time]         DATETIME     NULL,
    [updated_date_time]          DATETIME     NULL,
    [primary_employee_role_flag] BIT          NULL,
    [dv_load_date_time]          DATETIME     NOT NULL,
    [dv_batch_id]                BIGINT       NOT NULL,
    [dv_r_load_source_id]        BIGINT       NOT NULL,
    [dv_inserted_date_time]      DATETIME     NOT NULL,
    [dv_insert_user]             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]       DATETIME     NULL,
    [dv_update_user]             VARCHAR (50) NULL,
    [dv_hash]                    CHAR (32)    NOT NULL,
    [dv_deleted]                 INT          DEFAULT ((0)) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_employee_role]
    ON [dbo].[s_mms_employee_role]([bk_hash] ASC, [s_mms_employee_role_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_employee_role]([dv_batch_id] ASC);

