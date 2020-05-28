CREATE TABLE [dbo].[d_exerp_area_center] (
    [d_exerp_area_center_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)      NOT NULL,
    [center_id]              INT            NULL,
    [area_id]                INT            NULL,
    [area_center_tree_name]  VARCHAR (4000) NULL,
    [d_exerp_area_bk_hash]   CHAR (32)      NULL,
    [d_exerp_center_bk_hash] CHAR (32)      NULL,
    [p_exerp_area_center_id] BIGINT         NOT NULL,
    [deleted_flag]           INT            NULL,
    [dv_load_date_time]      DATETIME       NULL,
    [dv_load_end_date_time]  DATETIME       NULL,
    [dv_batch_id]            BIGINT         NOT NULL,
    [dv_inserted_date_time]  DATETIME       NOT NULL,
    [dv_insert_user]         VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]   DATETIME       NULL,
    [dv_update_user]         VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_exerp_area_center]([dv_batch_id] ASC);

