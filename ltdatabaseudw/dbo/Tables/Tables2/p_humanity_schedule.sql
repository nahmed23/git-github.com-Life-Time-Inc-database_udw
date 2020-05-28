CREATE TABLE [dbo].[p_humanity_schedule] (
    [p_humanity_schedule_id]               BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)     NOT NULL,
    [shift_id]                             BIGINT        NULL,
    [employee_id]                          VARCHAR (255) NULL,
    [position_id]                          VARCHAR (255) NULL,
    [ltf_file_name]                        VARCHAR (255) NULL,
    [l_humanity_schedule_id]               BIGINT        NULL,
    [s_humanity_schedule_id]               BIGINT        NULL,
    [dv_load_date_time]                    DATETIME      NOT NULL,
    [dv_load_end_date_time]                DATETIME      NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME      NULL,
    [dv_next_greatest_satellite_date_time] DATETIME      NULL,
    [dv_first_in_key_series]               INT           NULL,
    [dv_inserted_date_time]                DATETIME      NOT NULL,
    [dv_insert_user]                       VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                 DATETIME      NULL,
    [dv_update_user]                       VARCHAR (50)  NULL,
    [dv_batch_id]                          BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

