CREATE TABLE [dbo].[s_mart_dim_seg_member_primary_activity] (
    [s_mart_dim_seg_member_primary_activity_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)       NOT NULL,
    [dim_seg_member_primary_activity_id]        INT             NULL,
    [primary_activity_segment]                  DECIMAL (12, 4) NULL,
    [primary_activity]                          CHAR (30)       NULL,
    [row_add_date]                              DATETIME        NULL,
    [active_flag]                               INT             NULL,
    [dv_load_date_time]                         DATETIME        NOT NULL,
    [dv_r_load_source_id]                       BIGINT          NOT NULL,
    [dv_inserted_date_time]                     DATETIME        NOT NULL,
    [dv_insert_user]                            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                      DATETIME        NULL,
    [dv_update_user]                            VARCHAR (50)    NULL,
    [dv_hash]                                   CHAR (32)       NOT NULL,
    [dv_deleted]                                BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

