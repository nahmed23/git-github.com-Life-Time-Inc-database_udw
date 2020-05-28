CREATE TABLE [dbo].[s_exerp_activity] (
    [s_exerp_activity_id]           BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [activity_id]                   INT            NULL,
    [name]                          VARCHAR (4000) NULL,
    [state]                         VARCHAR (4000) NULL,
    [type]                          VARCHAR (4000) NULL,
    [color]                         VARCHAR (4000) NULL,
    [max_participants]              INT            NULL,
    [max_waiting_list_participants] INT            NULL,
    [description]                   VARCHAR (4000) NULL,
    [dummy_modified_date_time]      DATETIME       NULL,
    [dv_load_date_time]             DATETIME       NOT NULL,
    [dv_r_load_source_id]           BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL,
    [dv_hash]                       CHAR (32)      NOT NULL,
    [dv_deleted]                    BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exerp_activity]([dv_batch_id] ASC);

