CREATE TABLE [dbo].[d_boss_asi_class_r] (
    [d_boss_asi_class_r_id]         BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [class_r_dept]                  INT          NULL,
    [class_r_class]                 INT          NULL,
    [class_r_format_id]             INT          NULL,
    [class_r_interest_id]           INT          NULL,
    [class_r_updated_at]            DATETIME     NULL,
    [d_boss_interest_bk_hash]       VARCHAR (32) NULL,
    [d_boss_product_format_bk_hash] VARCHAR (32) NULL,
    [product_line]                  CHAR (30)    NULL,
    [updated_dim_date_key]          CHAR (8)     NULL,
    [updated_dim_time_key]          CHAR (8)     NULL,
    [p_boss_asi_class_r_id]         BIGINT       NOT NULL,
    [deleted_flag]                  INT          NULL,
    [dv_load_date_time]             DATETIME     NULL,
    [dv_load_end_date_time]         DATETIME     NULL,
    [dv_batch_id]                   BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

