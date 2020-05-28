CREATE TABLE [dbo].[dim_mms_member_30_day_rejoin] (
    [dim_mms_member_30_day_rejoin_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [date_of_birth]                   DATETIME      NULL,
    [dim_mms_member_key]              VARCHAR (32)  NULL,
    [email_address]                   VARCHAR (140) NULL,
    [entity_id]                       BIGINT        NULL,
    [first_name]                      VARCHAR (50)  NULL,
    [join_date]                       DATETIME      NULL,
    [last_name]                       VARCHAR (50)  NULL,
    [member_active_flag]              CHAR (1)      NULL,
    [member_id]                       INT           NULL,
    [member_type]                     VARCHAR (50)  NULL,
    [phone_number]                    VARCHAR (40)  NULL,
    [previous_join_date]              DATETIME      NULL,
    [sex]                             VARCHAR (1)   NULL,
    [termination_reason]              VARCHAR (50)  NULL,
    [dv_load_date_time]               DATETIME      NULL,
    [dv_load_end_date_time]           DATETIME      NULL,
    [dv_batch_id]                     BIGINT        NOT NULL,
    [dv_inserted_date_time]           DATETIME      NOT NULL,
    [dv_insert_user]                  VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]            DATETIME      NULL,
    [dv_update_user]                  VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

