CREATE TABLE [dbo].[d_mart_dim_seg_member_lifecycle] (
    [d_mart_dim_seg_member_lifecycle_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)       NOT NULL,
    [dim_seg_member_lifecycle_key]       VARCHAR (32)    NULL,
    [dim_seg_member_lifecycle_id]        INT             NULL,
    [active_flag]                        CHAR (1)        NULL,
    [lifecycle]                          CHAR (20)       NULL,
    [lifecycle_segment]                  DECIMAL (26, 6) NULL,
    [row_add_date]                       DATETIME        NULL,
    [row_add_dim_date_key]               VARCHAR (8)     NULL,
    [row_add_dim_time_key]               INT             NULL,
    [p_mart_dim_seg_member_lifecycle_id] BIGINT          NOT NULL,
    [deleted_flag]                       INT             NULL,
    [dv_load_date_time]                  DATETIME        NULL,
    [dv_load_end_date_time]              DATETIME        NULL,
    [dv_batch_id]                        BIGINT          NOT NULL,
    [dv_inserted_date_time]              DATETIME        NOT NULL,
    [dv_insert_user]                     VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]               DATETIME        NULL,
    [dv_update_user]                     VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

