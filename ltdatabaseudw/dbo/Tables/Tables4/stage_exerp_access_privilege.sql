CREATE TABLE [dbo].[stage_exerp_access_privilege] (
    [stage_exerp_access_privilege_id] BIGINT         NOT NULL,
    [id]                              INT            NULL,
    [privilege_set_id]                INT            NULL,
    [access_group_id]                 INT            NULL,
    [scope_type]                      VARCHAR (4000) NULL,
    [scope_id]                        INT            NULL,
    [dummy_modified_date_time]        DATETIME       NULL,
    [dv_batch_id]                     BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

