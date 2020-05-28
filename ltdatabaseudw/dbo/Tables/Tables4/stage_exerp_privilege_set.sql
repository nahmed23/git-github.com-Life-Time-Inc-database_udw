CREATE TABLE [dbo].[stage_exerp_privilege_set] (
    [stage_exerp_privilege_set_id] BIGINT         NOT NULL,
    [id]                           INT            NULL,
    [name]                         VARCHAR (4000) NULL,
    [description]                  VARCHAR (4000) NULL,
    [privilege_set_group_id]       INT            NULL,
    [privilege_set_group_name]     VARCHAR (4000) NULL,
    [scope_type]                   VARCHAR (4000) NULL,
    [scope_id]                     INT            NULL,
    [dummy_modified_date_time]     DATETIME       NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

