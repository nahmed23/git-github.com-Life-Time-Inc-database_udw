CREATE TABLE [dbo].[s_exerp_clipcard] (
    [s_exerp_clipcard_id]      BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)      NOT NULL,
    [clipcard_id]              VARCHAR (4000) NULL,
    [clips_left]               INT            NULL,
    [clips_initial]            INT            NULL,
    [valid_from_datetime]      DATETIME       NULL,
    [valid_until_datetime]     DATETIME       NULL,
    [blocked]                  BIT            NULL,
    [cancelled]                BIT            NULL,
    [cancel_datetime]          DATETIME       NULL,
    [ets]                      BIGINT         NULL,
    [dummy_modified_date_time] DATETIME       NULL,
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

