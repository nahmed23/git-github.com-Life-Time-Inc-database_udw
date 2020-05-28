CREATE TABLE [dbo].[stage_hash_exerp_area_center] (
    [stage_hash_exerp_area_center_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)      NOT NULL,
    [center_id]                       INT            NULL,
    [area_id]                         INT            NULL,
    [tree_name]                       VARCHAR (4000) NULL,
    [dummy_modified_date_time]        DATETIME       NULL,
    [dv_load_date_time]               DATETIME       NOT NULL,
    [dv_batch_id]                     BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

