CREATE TABLE [dbo].[s_ec_workouts] (
    [s_ec_workouts_id]      BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)      NOT NULL,
    [workouts_id]           INT            NULL,
    [name]                  NVARCHAR (100) NULL,
    [description]           VARCHAR (8000) NULL,
    [created_date]          DATETIME       NULL,
    [modified_date]         DATETIME       NULL,
    [inactive_date]         DATETIME       NULL,
    [tags]                  VARCHAR (8000) NULL,
    [type]                  INT            NULL,
    [discriminator]         NVARCHAR (128) NULL,
    [dv_load_date_time]     DATETIME       NOT NULL,
    [dv_r_load_source_id]   BIGINT         NOT NULL,
    [dv_inserted_date_time] DATETIME       NOT NULL,
    [dv_insert_user]        VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]  DATETIME       NULL,
    [dv_update_user]        VARCHAR (50)   NULL,
    [dv_hash]               CHAR (32)      NOT NULL,
    [dv_deleted]            BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]           BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

