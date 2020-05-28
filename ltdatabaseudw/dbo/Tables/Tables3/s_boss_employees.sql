CREATE TABLE [dbo].[s_boss_employees] (
    [s_boss_employees_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [last]                  NCHAR (25)      NULL,
    [first]                 NCHAR (20)      NULL,
    [MI]                    CHAR (1)        NULL,
    [badge]                 CHAR (10)       NULL,
    [status]                CHAR (1)        NULL,
    [email]                 VARCHAR (240)   NULL,
    [user_profile]          VARCHAR (240)   NULL,
    [nickname]              CHAR (30)       NULL,
    [cost]                  DECIMAL (26, 6) NULL,
    [employee_url]          VARCHAR (80)    NULL,
    [employee_id]           INT             NULL,
    [phone]                 VARCHAR (20)    NULL,
    [res_color]             INT             NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_employees]([dv_batch_id] ASC);

