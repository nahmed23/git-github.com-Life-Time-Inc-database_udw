CREATE TABLE [dbo].[d_exerp_area] (
    [d_exerp_area_id]             BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)      NOT NULL,
    [area_id]                     INT            NULL,
    [area_blocked_flag]           CHAR (1)       NULL,
    [area_name]                   VARCHAR (4000) NULL,
    [area_tree_name]              VARCHAR (4000) NULL,
    [parent_area_id]              INT            NULL,
    [parent_d_exerp_area_bk_hash] CHAR (32)      NULL,
    [p_exerp_area_id]             BIGINT         NOT NULL,
    [deleted_flag]                INT            NULL,
    [dv_load_date_time]           DATETIME       NULL,
    [dv_load_end_date_time]       DATETIME       NULL,
    [dv_batch_id]                 BIGINT         NOT NULL,
    [dv_inserted_date_time]       DATETIME       NOT NULL,
    [dv_insert_user]              VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]        DATETIME       NULL,
    [dv_update_user]              VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_exerp_area]([dv_batch_id] ASC);

