CREATE TABLE [dbo].[p_exerp_message] (
    [p_exerp_message_id]                   BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)      NOT NULL,
    [message_id]                           VARCHAR (4000) NULL,
    [l_exerp_message_id]                   BIGINT         NULL,
    [s_exerp_message_id]                   BIGINT         NULL,
    [dv_load_date_time]                    DATETIME       NOT NULL,
    [dv_load_end_date_time]                DATETIME       NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME       NULL,
    [dv_next_greatest_satellite_date_time] DATETIME       NULL,
    [dv_first_in_key_series]               INT            NULL,
    [dv_inserted_date_time]                DATETIME       NOT NULL,
    [dv_insert_user]                       VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                 DATETIME       NULL,
    [dv_update_user]                       VARCHAR (50)   NULL,
    [dv_batch_id]                          BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_exerp_message]([dv_batch_id] ASC);

