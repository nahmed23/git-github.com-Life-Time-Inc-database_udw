CREATE TABLE [dbo].[p_exacttarget_sent] (
    [p_exacttarget_sent_id]                BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [client_id]                            BIGINT       NULL,
    [send_id]                              BIGINT       NULL,
    [subscriber_id]                        BIGINT       NULL,
    [list_id]                              BIGINT       NULL,
    [batch_id]                             BIGINT       NULL,
    [s_exacttarget_sent_id]                BIGINT       NULL,
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
CREATE CLUSTERED COLUMNSTORE INDEX [cci_p_exacttarget_sent]
    ON [dbo].[p_exacttarget_sent];


GO
CREATE STATISTICS [stat_p_exacttarget_sent]
    ON [dbo].[p_exacttarget_sent]([dv_batch_id]);

