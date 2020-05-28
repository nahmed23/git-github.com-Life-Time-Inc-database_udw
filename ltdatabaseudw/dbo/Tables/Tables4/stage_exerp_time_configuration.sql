CREATE TABLE [dbo].[stage_exerp_time_configuration] (
    [stage_exerp_time_configuration_id] BIGINT         NOT NULL,
    [id]                                INT            NULL,
    [name]                              VARCHAR (4000) NULL,
    [part_from]                         INT            NULL,
    [part_from_unit]                    VARCHAR (4000) NULL,
    [part_staff_stop]                   INT            NULL,
    [part_staff_stop_unit]              VARCHAR (4000) NULL,
    [part_cust_stop]                    INT            NULL,
    [part_cust_stop_unit]               VARCHAR (4000) NULL,
    [cancel_sanc_start]                 INT            NULL,
    [cancel_sanc_start_unit]            VARCHAR (4000) NULL,
    [cancel_stop_staff]                 INT            NULL,
    [cancel_stop_staff_unit]            VARCHAR (4000) NULL,
    [cancel_stop_cust]                  INT            NULL,
    [cancel_stop_cust_unit]             VARCHAR (4000) NULL,
    [recurrence_in_past]                INT            NULL,
    [recurrence_in_past_unit]           VARCHAR (4000) NULL,
    [course_sign_start]                 INT            NULL,
    [course_sign_start_unit]            VARCHAR (4000) NULL,
    [course_stop]                       INT            NULL,
    [course_stop_unit]                  VARCHAR (4000) NULL,
    [dummy_modified_date_time]          DATETIME       NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

