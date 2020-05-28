CREATE TABLE [dbo].[s_boss_asi_club_res] (
    [s_boss_asi_club_res_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)     NOT NULL,
    [club]                   INT           NULL,
    [resource_id]            INT           NULL,
    [status]                 CHAR (1)      NULL,
    [resource_type]          CHAR (25)     NULL,
    [resource]               CHAR (25)     NULL,
    [comment]                VARCHAR (40)  NULL,
    [square_feet]            INT           NULL,
    [created_at]             DATETIME      NULL,
    [updated_at]             DATETIME      NULL,
    [capacity]               INT           NULL,
    [web_enable]             CHAR (1)      NULL,
    [web_start_date]         DATETIME      NULL,
    [web_active]             CHAR (1)      NULL,
    [inactive_start_date]    DATETIME      NULL,
    [inactive_end_date]      DATETIME      NULL,
    [phone]                  VARCHAR (12)  NULL,
    [floor]                  INT           NULL,
    [web_description]        VARCHAR (400) NULL,
    [support_phone]          VARCHAR (12)  NULL,
    [support_email]          VARCHAR (240) NULL,
    [resource_phone]         VARCHAR (12)  NULL,
    [dv_load_date_time]      DATETIME      NOT NULL,
    [dv_r_load_source_id]    BIGINT        NOT NULL,
    [dv_inserted_date_time]  DATETIME      NOT NULL,
    [dv_insert_user]         VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]   DATETIME      NULL,
    [dv_update_user]         VARCHAR (50)  NULL,
    [dv_hash]                CHAR (32)     NOT NULL,
    [dv_batch_id]            BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_asi_club_res]([dv_batch_id] ASC);

