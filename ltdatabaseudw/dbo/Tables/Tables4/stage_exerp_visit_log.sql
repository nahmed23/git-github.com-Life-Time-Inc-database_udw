CREATE TABLE [dbo].[stage_exerp_visit_log] (
    [stage_exerp_visit_log_id] BIGINT         NOT NULL,
    [id]                       INT            NULL,
    [center_id]                INT            NULL,
    [person_id]                VARCHAR (4000) NULL,
    [home_center_id]           INT            NULL,
    [check_in_datetime]        DATETIME       NULL,
    [check_out_datetime]       DATETIME       NULL,
    [result]                   VARCHAR (4000) NULL,
    [card_checked_in]          BIT            NULL,
    [ets]                      BIGINT         NULL,
    [dummy_modified_date_time] DATETIME       NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

