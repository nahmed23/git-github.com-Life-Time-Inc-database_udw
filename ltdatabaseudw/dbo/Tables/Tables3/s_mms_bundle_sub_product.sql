CREATE TABLE [dbo].[s_mms_bundle_sub_product] (
    [s_mms_bundle_sub_product_id]           BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)    NOT NULL,
    [bundle_sub_product_id]                 INT          NULL,
    [bundle_product_group_number]           INT          NULL,
    [quantity]                              INT          NULL,
    [gl_account_number]                     VARCHAR (5)  NULL,
    [gl_sub_account_number]                 VARCHAR (7)  NULL,
    [inserted_date_time]                    DATETIME     NULL,
    [updated_date_time]                     DATETIME     NULL,
    [workday_account]                       VARCHAR (6)  NULL,
    [workday_cost_center]                   VARCHAR (6)  NULL,
    [workday_offering]                      VARCHAR (10) NULL,
    [workday_over_ride_region]              VARCHAR (4)  NULL,
    [workday_revenue_product_group_account] VARCHAR (6)  NULL,
    [dv_load_date_time]                     DATETIME     NOT NULL,
    [dv_batch_id]                           BIGINT       NOT NULL,
    [dv_r_load_source_id]                   BIGINT       NOT NULL,
    [dv_inserted_date_time]                 DATETIME     NOT NULL,
    [dv_insert_user]                        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                  DATETIME     NULL,
    [dv_update_user]                        VARCHAR (50) NULL,
    [dv_hash]                               CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_bundle_sub_product]
    ON [dbo].[s_mms_bundle_sub_product]([bk_hash] ASC, [s_mms_bundle_sub_product_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_bundle_sub_product]([dv_batch_id] ASC);

