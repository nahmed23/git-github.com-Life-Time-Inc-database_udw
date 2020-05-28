CREATE TABLE [dbo].[s_mms_employee] (
    [s_mms_employee_id]     BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [employee_id]           INT          NULL,
    [active_status_flag]    BIT          NULL,
    [first_name]            VARCHAR (50) NULL,
    [last_name]             VARCHAR (50) NULL,
    [middle_int]            VARCHAR (3)  NULL,
    [inserted_date_time]    DATETIME     NULL,
    [updated_date_time]     DATETIME     NULL,
    [hire_date]             DATETIME     NULL,
    [termination_date]      DATETIME     NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_employee]
    ON [dbo].[s_mms_employee]([bk_hash] ASC, [s_mms_employee_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_employee]([dv_batch_id] ASC);

