CREATE TABLE [dbo].[audit_bank_recon_table] (
    [dv_job_name]                 VARCHAR (100) NULL,
    [dv_job_group]                VARCHAR (100) NULL,
    [begin_extract_date_time]     DATETIME      NULL,
    [utc_begin_extract_date_time] DATETIME      NULL,
    [report_start_date]           VARCHAR (100) NULL,
    [report_end_date]             VARCHAR (100) NULL,
    [job_start_date_time]         DATETIME      NULL,
    [source_file_name]            VARCHAR (255) NULL,
    [dv_batch_id]                 BIGINT        NULL,
    [dv_inserted_date_time]       DATETIME      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

