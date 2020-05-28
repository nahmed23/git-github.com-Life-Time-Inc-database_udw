CREATE TABLE [dbo].[stage_exerp_activity_override] (
    [stage_exerp_activity_override_id] BIGINT         NOT NULL,
    [id]                               INT            NULL,
    [activity_id]                      INT            NULL,
    [name]                             VARCHAR (4000) NULL,
    [time_configuration_id]            INT            NULL,
    [age_group_id]                     INT            NULL,
    [center_id]                        INT            NULL,
    [dummy_modified_date_time]         DATETIME       NULL,
    [dv_batch_id]                      BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

