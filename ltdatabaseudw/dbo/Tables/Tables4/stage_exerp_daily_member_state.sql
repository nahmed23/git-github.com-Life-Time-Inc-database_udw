CREATE TABLE [dbo].[stage_exerp_daily_member_state] (
    [stage_exerp_daily_member_state_id] BIGINT         NOT NULL,
    [id]                                INT            NULL,
    [person_id]                         VARCHAR (4000) NULL,
    [center_id]                         INT            NULL,
    [home_center_person_id]             INT            NULL,
    [date]                              DATETIME       NULL,
    [entry_datetime]                    DATETIME       NULL,
    [change]                            VARCHAR (4000) NULL,
    [member_number_delta]               INT            NULL,
    [extra_number_delta]                INT            NULL,
    [secondary_member_number_delta]     INT            NULL,
    [cancel_datetime]                   DATETIME       NULL,
    [ets]                               BIGINT         NULL,
    [dummy_modified_date_time]          DATETIME       NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

