CREATE TABLE [dbo].[stage_exerp_debt_case] (
    [stage_exerp_debt_case_id] BIGINT           NOT NULL,
    [id]                       VARCHAR (4000)   NULL,
    [center_id]                INT              NULL,
    [person_id]                VARCHAR (4000)   NULL,
    [company_id]               VARCHAR (4000)   NULL,
    [start_datetime]           DATETIME         NULL,
    [amount]                   NUMERIC (18, 10) NULL,
    [closed]                   BIT              NULL,
    [closed_datetime]          DATETIME         NULL,
    [current_step]             INT              NULL,
    [ets]                      BIGINT           NULL,
    [dummy_modified_date_time] DATETIME         NULL,
    [dv_batch_id]              BIGINT           NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

