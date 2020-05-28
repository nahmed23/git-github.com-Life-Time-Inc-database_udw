CREATE TABLE [dbo].[d_mms_employee_history] (
    [d_mms_employee_history_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)     NOT NULL,
    [dim_employee_key]          CHAR (32)     NULL,
    [employee_id]               INT           NULL,
    [effective_date_time]       DATETIME      NULL,
    [expiration_date_time]      DATETIME      NULL,
    [dim_club_key]              CHAR (32)     NULL,
    [employee_active_flag]      VARCHAR (50)  NULL,
    [employee_name]             VARCHAR (101) NULL,
    [employee_name_last_first]  VARCHAR (101) NULL,
    [first_name]                VARCHAR (50)  NULL,
    [inserted_date_time]        DATETIME      NULL,
    [last_name]                 VARCHAR (50)  NULL,
    [member_id]                 INT           NULL,
    [p_mms_employee_id]         BIGINT        NOT NULL,
    [deleted_flag]              INT           NULL,
    [dv_load_date_time]         DATETIME      NULL,
    [dv_load_end_date_time]     DATETIME      NULL,
    [dv_batch_id]               BIGINT        NOT NULL,
    [dv_inserted_date_time]     DATETIME      NOT NULL,
    [dv_insert_user]            VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]      DATETIME      NULL,
    [dv_update_user]            VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_employee_history]([dv_batch_id] ASC);

