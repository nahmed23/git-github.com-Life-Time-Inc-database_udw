CREATE TABLE [dbo].[p_exacttarget_send_log] (
    [p_exacttarget_send_log_id]            BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [job_id]                               BIGINT       NULL,
    [list_id]                              BIGINT       NULL,
    [batch_id]                             BIGINT       NULL,
    [sub_id]                               BIGINT       NULL,
    [triggered_send_id]                    BIGINT       NULL,
    [member_id]                            BIGINT       NULL,
    [s_exacttarget_send_log_id]            BIGINT       NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_first_in_key_series]               INT          NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED COLUMNSTORE INDEX [cci_p_exacttarget_send_log]
    ON [dbo].[p_exacttarget_send_log];

