CREATE TABLE [dbo].[stage_hash_boss_employees] (
    [stage_hash_boss_employees_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)     NOT NULL,
    [last]                         NCHAR (25)    NULL,
    [first]                        NCHAR (20)    NULL,
    [MI]                           CHAR (1)      NULL,
    [interestID]                   INT           NULL,
    [home_club]                    INT           NULL,
    [badge]                        CHAR (10)     NULL,
    [roleID]                       INT           NULL,
    [status]                       CHAR (1)      NULL,
    [email]                        VARCHAR (240) NULL,
    [user_profile]                 VARCHAR (240) NULL,
    [nickname]                     CHAR (30)     NULL,
    [cost]                         MONEY         NULL,
    [employee_url]                 VARCHAR (80)  NULL,
    [employee_id]                  INT           NULL,
    [id]                           INT           NULL,
    [member_ID]                    CHAR (10)     NULL,
    [phone]                        VARCHAR (20)  NULL,
    [res_color]                    INT           NULL,
    [jan_one]                      DATETIME      NULL,
    [dv_load_date_time]            DATETIME      NOT NULL,
    [dv_updated_date_time]         DATETIME      NULL,
    [dv_update_user]               VARCHAR (50)  NULL,
    [dv_batch_id]                  BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

