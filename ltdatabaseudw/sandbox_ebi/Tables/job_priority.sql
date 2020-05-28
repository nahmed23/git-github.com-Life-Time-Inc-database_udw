CREATE TABLE [sandbox_ebi].[job_priority] (
    [dv_job_status_id]     INT             NULL,
    [job_name]             VARCHAR (200)   NULL,
    [job_category]         VARCHAR (200)   NULL,
    [job_priority_rank]    DECIMAL (10, 5) NULL,
    [priority_note]        VARCHAR (MAX)   NULL,
    [LastModifiedDateTime] DATETIME        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_job_status_id]));

