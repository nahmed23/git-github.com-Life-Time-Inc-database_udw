CREATE TABLE [dbo].[s_boss_asi_class_r] (
    [s_boss_asi_class_r_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [class_r_dept]          INT             NULL,
    [class_r_class]         INT             NULL,
    [class_r_desc]          CHAR (30)       NULL,
    [class_r_unit_order]    INT             NULL,
    [class_r_unit_sale]     INT             NULL,
    [class_r_comm_part]     CHAR (1)        NULL,
    [class_r_comm_percent]  DECIMAL (5, 3)  NULL,
    [class_r_comm_amt]      DECIMAL (26, 6) NULL,
    [class_r_promo_part]    CHAR (1)        NULL,
    [class_r_suggestion]    CHAR (48)       NULL,
    [class_r_size_name]     CHAR (25)       NULL,
    [class_r_color_name]    CHAR (25)       NULL,
    [class_r_style_name]    CHAR (25)       NULL,
    [class_r_type]          CHAR (1)        NULL,
    [class_r_gl_acct]       CHAR (4)        NULL,
    [class_r_future_acct]   CHAR (4)        NULL,
    [class_r_tax_rate]      DECIMAL (4, 3)  NULL,
    [class_r_bill_hrs]      CHAR (1)        NULL,
    [class_r_web_publish]   CHAR (1)        NULL,
    [class_r_sort_order]    INT             NULL,
    [class_r_created_at]    DATETIME        NULL,
    [class_r_updated_at]    DATETIME        NULL,
    [class_r_grace_days]    INT             NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_boss_asi_class_r]
    ON [dbo].[s_boss_asi_class_r]([bk_hash] ASC, [s_boss_asi_class_r_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_asi_class_r]([dv_batch_id] ASC);

