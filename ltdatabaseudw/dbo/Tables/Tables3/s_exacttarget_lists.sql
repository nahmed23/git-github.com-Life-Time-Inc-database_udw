CREATE TABLE [dbo].[s_exacttarget_lists] (
    [s_exacttarget_lists_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)      NOT NULL,
    [client_id]              BIGINT         NULL,
    [list_id]                BIGINT         NULL,
    [name]                   VARCHAR (4000) NULL,
    [description]            VARCHAR (4000) NULL,
    [date_created]           DATETIME       NULL,
    [status]                 VARCHAR (4000) NULL,
    [list_type]              VARCHAR (4000) NULL,
    [jan_one]                DATETIME       NULL,
    [dv_load_date_time]      DATETIME       NOT NULL,
    [dv_batch_id]            BIGINT         NOT NULL,
    [dv_r_load_source_id]    BIGINT         NOT NULL,
    [dv_inserted_date_time]  DATETIME       NOT NULL,
    [dv_insert_user]         VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]   DATETIME       NULL,
    [dv_update_user]         VARCHAR (50)   NULL,
    [dv_hash]                CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_exacttarget_lists]
    ON [dbo].[s_exacttarget_lists]([bk_hash] ASC, [s_exacttarget_lists_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exacttarget_lists]([dv_batch_id] ASC);

