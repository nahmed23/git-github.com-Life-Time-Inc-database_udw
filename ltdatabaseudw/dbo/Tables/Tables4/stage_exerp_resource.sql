CREATE TABLE [dbo].[stage_exerp_resource] (
    [stage_exerp_resource_id]  BIGINT         NOT NULL,
    [id]                       VARCHAR (4000) NULL,
    [name]                     VARCHAR (4000) NULL,
    [state]                    VARCHAR (4000) NULL,
    [type]                     VARCHAR (4000) NULL,
    [access_group_name]        VARCHAR (4000) NULL,
    [external_id]              VARCHAR (4000) NULL,
    [center_id]                INT            NULL,
    [access_group_id]          INT            NULL,
    [comment]                  VARCHAR (4000) NULL,
    [show_calendar]            BIT            NULL,
    [dummy_modified_date_time] DATETIME       NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

