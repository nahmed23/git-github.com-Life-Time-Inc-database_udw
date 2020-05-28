CREATE TABLE [dbo].[p_exacttarget_send_jobs] (
    [p_exacttarget_send_jobs_id]           BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [client_id]                            BIGINT       NULL,
    [send_id]                              BIGINT       NULL,
    [s_exacttarget_send_jobs_id]           BIGINT       NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_batch_id]                          BIGINT       NOT NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_first_in_key_series]               BIT          NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_exacttarget_send_jobs]
    ON [dbo].[p_exacttarget_send_jobs]([bk_hash] ASC, [p_exacttarget_send_jobs_id] ASC);


GO
CREATE STATISTICS [stat_p_exacttarget_send_jobs]
    ON [dbo].[p_exacttarget_send_jobs]([dv_batch_id]);

