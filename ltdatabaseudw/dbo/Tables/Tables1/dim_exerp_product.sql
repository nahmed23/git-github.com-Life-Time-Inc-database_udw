﻿CREATE TABLE [dbo].[dim_exerp_product] (
    [dim_exerp_product_id]                 BIGINT          IDENTITY (1, 1) NOT NULL,
    [blocked]                              CHAR (1)        NULL,
    [cost_price]                           DECIMAL (26, 6) NULL,
    [dim_club_key]                         CHAR (32)       NULL,
    [dim_exerp_product_key]                CHAR (32)       NULL,
    [dim_mms_product_key]                  VARCHAR (32)    NULL,
    [external_id]                          VARCHAR (4000)  NULL,
    [flat_rate_commission]                 DECIMAL (26, 6) NULL,
    [included_member_count]                INT             NULL,
    [master_dim_exerp_product_key]         CHAR (32)       NULL,
    [master_product_global_id]             VARCHAR (4000)  NULL,
    [master_product_id]                    INT             NULL,
    [master_product_name]                  VARCHAR (4000)  NULL,
    [master_product_state]                 VARCHAR (4000)  NULL,
    [minimum_price]                        DECIMAL (26, 6) NULL,
    [period_commission]                    INT             NULL,
    [primary_dimension_product_group_id]   INT             NULL,
    [primary_dimension_product_group_name] VARCHAR (4000)  NULL,
    [primary_parent_product_group_id]      INT             NULL,
    [primary_parent_product_group_name]    VARCHAR (4000)  NULL,
    [primary_product_group_external_id]    VARCHAR (4000)  NULL,
    [primary_product_group_id]             INT             NULL,
    [primary_product_group_name]           VARCHAR (4000)  NULL,
    [product_id]                           VARCHAR (4000)  NULL,
    [product_name]                         VARCHAR (4000)  NULL,
    [product_type]                         VARCHAR (4000)  NULL,
    [sales_commission]                     INT             NULL,
    [sales_price]                          DECIMAL (26, 6) NULL,
    [sales_units]                          INT             NULL,
    [dv_load_date_time]                    DATETIME        NULL,
    [dv_load_end_date_time]                DATETIME        NULL,
    [dv_batch_id]                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                DATETIME        NOT NULL,
    [dv_insert_user]                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                 DATETIME        NULL,
    [dv_update_user]                       VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_exerp_product_key]));

