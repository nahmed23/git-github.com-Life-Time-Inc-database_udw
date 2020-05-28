CREATE TABLE [dbo].[s_exerp_activity_group] (
    [s_exerp_activity_group_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [activity_group_id]         INT            NULL,
    [name]                      VARCHAR (4000) NULL,
    [state]                     VARCHAR (4000) NULL,
    [book_kiosk]                INT            NULL,
    [book_web]                  INT            NULL,
    [book_api]                  INT            NULL,
    [book_mobile_api]           INT            NULL,
    [book_client]               INT            NULL,
    [dummy_modified_date_time]  DATETIME       NULL,
    [dv_load_date_time]         DATETIME       NOT NULL,
    [dv_r_load_source_id]       BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL,
    [dv_hash]                   CHAR (32)      NOT NULL,
    [dv_deleted]                BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]               BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exerp_activity_group]([dv_batch_id] ASC);

