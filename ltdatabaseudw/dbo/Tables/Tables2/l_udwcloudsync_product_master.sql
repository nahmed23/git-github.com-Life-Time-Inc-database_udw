﻿CREATE TABLE [dbo].[l_udwcloudsync_product_master] (
    [l_udwcloudsync_product_master_id]                  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                           CHAR (32)       NOT NULL,
    [product_discount_gl_account]                       NVARCHAR (4000) NULL,
    [product_gl_account]                                NVARCHAR (4000) NULL,
    [product_gl_department_code]                        NVARCHAR (4000) NULL,
    [product_gl_product_code]                           NVARCHAR (4000) NULL,
    [product_id]                                        NVARCHAR (4000) NULL,
    [product_refund_gl_account]                         NVARCHAR (4000) NULL,
    [product_sku]                                       NVARCHAR (4000) NULL,
    [product_workday_account]                           NVARCHAR (4000) NULL,
    [product_workday_cost_center]                       NVARCHAR (4000) NULL,
    [product_workday_discount_gl_account]               NVARCHAR (4000) NULL,
    [product_workday_refund_gl_account]                 NVARCHAR (4000) NULL,
    [revenue_product_group_discount_gl_account]         NVARCHAR (4000) NULL,
    [revenue_product_group_gl_account]                  NVARCHAR (4000) NULL,
    [revenue_product_group_refund_gl_account]           NVARCHAR (4000) NULL,
    [source_system_link_title]                          NVARCHAR (4000) NULL,
    [workday_revenue_product_group_account]             NVARCHAR (4000) NULL,
    [workday_revenue_product_group_discount_gl_account] NVARCHAR (4000) NULL,
    [workday_revenue_product_group_refund_gl_account]   NVARCHAR (4000) NULL,
    [dv_load_date_time]                                 DATETIME        NOT NULL,
    [dv_r_load_source_id]                               BIGINT          NOT NULL,
    [dv_inserted_date_time]                             DATETIME        NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                              DATETIME        NULL,
    [dv_update_user]                                    VARCHAR (50)    NULL,
    [dv_hash]                                           CHAR (32)       NOT NULL,
    [dv_batch_id]                                       BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_udwcloudsync_product_master]([dv_batch_id] ASC);

