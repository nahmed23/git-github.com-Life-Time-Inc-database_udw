CREATE TABLE [dbo].[stage_exerp_activity] (
    [stage_exerp_activity_id]       BIGINT         NOT NULL,
    [id]                            INT            NULL,
    [name]                          VARCHAR (4000) NULL,
    [state]                         VARCHAR (4000) NULL,
    [type]                          VARCHAR (4000) NULL,
    [activity_group_id]             INT            NULL,
    [color]                         VARCHAR (4000) NULL,
    [max_participants]              INT            NULL,
    [max_waiting_list_participants] INT            NULL,
    [external_id]                   VARCHAR (4000) NULL,
    [access_group_id]               INT            NULL,
    [description]                   VARCHAR (4000) NULL,
    [time_configuration_id]         INT            NULL,
    [course_schedule_type]          VARCHAR (4000) NULL,
    [age_group_id]                  INT            NULL,
    [dummy_modified_date_time]      DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

