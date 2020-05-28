CREATE TABLE [dbo].[l_exerp_staff_usage_1] (
    [l_exerp_staff_usage_1_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)      NOT NULL,
    [staff_usage_id]           INT            NULL,
    [substitute_of_person_id]  VARCHAR (4000) NULL,
    [dv_load_date_time]        DATETIME       NOT NULL,
    [dv_r_load_source_id]      BIGINT         NOT NULL,
    [dv_inserted_date_time]    DATETIME       NOT NULL,
    [dv_insert_user]           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]     DATETIME       NULL,
    [dv_update_user]           VARCHAR (50)   NULL,
    [dv_hash]                  CHAR (32)      NOT NULL,
    [dv_deleted]               BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

