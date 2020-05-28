CREATE TABLE [dbo].[stage_loc_log_detail] (
    [stage_loc_log_detail_seq]     BIGINT        NULL,
    [record_count]                 BIGINT        NULL,
    [table_name]                   VARCHAR (255) NULL,
    [workflow_name]                VARCHAR (255) NULL,
    [job_start_date_time]          DATETIME      NULL,
    [job_end_date_time]            DATETIME      NULL,
    [begin_extract_date_time]      DATETIME      NULL,
    [next_begin_extract_date_time] DATETIME      NULL,
    [dv_batch_id]                  BIGINT        NULL,
    [Standard_deviation]           INT           NULL,
    [average]                      INT           NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([stage_loc_log_detail_seq]));

