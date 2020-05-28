CREATE TABLE [dbo].[d_mms_drawer_activity] (
    [d_mms_drawer_activity_id]     BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)     NOT NULL,
    [dim_mms_drawer_activity_key]  CHAR (32)     NULL,
    [drawer_activity_id]           INT           NULL,
    [closed_business_dim_date_key] INT           NULL,
    [closed_dim_date_key]          INT           NULL,
    [closed_dim_employee_key]      CHAR (32)     NULL,
    [closed_dim_time_key]          INT           NULL,
    [closed_flag]                  CHAR (1)      NULL,
    [closing_comments]             VARCHAR (527) NULL,
    [d_mms_drawer_bk_hash]         CHAR (32)     NULL,
    [open_dim_date_key]            INT           NULL,
    [open_dim_employee_key]        CHAR (32)     NULL,
    [open_dim_time_key]            INT           NULL,
    [open_flag]                    CHAR (1)      NULL,
    [pending_dim_date_key]         INT           NULL,
    [pending_dim_employee_key]     CHAR (32)     NULL,
    [pending_dim_time_key]         INT           NULL,
    [pending_flag]                 CHAR (1)      NULL,
    [p_mms_drawer_activity_id]     BIGINT        NOT NULL,
    [dv_load_date_time]            DATETIME      NULL,
    [dv_load_end_date_time]        DATETIME      NULL,
    [dv_batch_id]                  BIGINT        NOT NULL,
    [dv_inserted_date_time]        DATETIME      NOT NULL,
    [dv_insert_user]               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]         DATETIME      NULL,
    [dv_update_user]               VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_drawer_activity]([dv_batch_id] ASC);

