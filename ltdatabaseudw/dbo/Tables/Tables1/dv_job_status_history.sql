CREATE TABLE [dbo].[dv_job_status_history] (
    [dv_job_status_history_id]         BIGINT        IDENTITY (1, 1) NOT NULL,
    [dv_job_status_id]                 BIGINT        NOT NULL,
    [job_name]                         VARCHAR (256) NOT NULL,
    [job_start_date_time]              DATETIME      NULL,
    [job_end_date_time]                DATETIME      NULL,
    [job_status]                       VARCHAR (256) NOT NULL,
    [begin_extract_date_time]          DATETIME      NOT NULL,
    [utc_begin_extract_date_time]      DATETIME      NOT NULL,
    [next_begin_extract_date_time]     DATETIME      NOT NULL,
    [next_utc_begin_extract_date_time] DATETIME      NOT NULL,
    [source_name]                      VARCHAR (256) NOT NULL,
    [job_group]                        VARCHAR (256) NOT NULL,
    [dv_inserted_date_time]            DATETIME      NOT NULL,
    [dv_insert_user]                   VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]             DATETIME      NULL,
    [dv_update_user]                   VARCHAR (50)  NULL,
    [dv_batch_id]                      BIGINT        NOT NULL,
    [enabled_flag]                     BIT           NOT NULL,
    [retry_flag]                       BIT           NOT NULL,
    [job_priority]                     INT           NOT NULL,
    [informatica_folder_name]          VARCHAR (256) NOT NULL,
    [dispatcher_id]                    INT           NULL,
    [dispatcher_ignore_flag]           BIT           NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[dv_job_status_history]([dv_batch_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_job_name]
    ON [dbo].[dv_job_status_history]([job_name] ASC);

