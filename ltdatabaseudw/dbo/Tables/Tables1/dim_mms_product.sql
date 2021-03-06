﻿CREATE TABLE [dbo].[dim_mms_product] (
    [dim_mms_product_id]                                BIGINT         IDENTITY (1, 1) NOT NULL,
    [dim_mms_product_key]                               CHAR (32)      NULL,
    [product_id]                                        INT            NULL,
    [access_by_price_paid_flag]                         CHAR (1)       NULL,
    [assess_as_dues_flag]                               CHAR (1)       NULL,
    [default_dim_reporting_hierarchy_key]               VARCHAR (32)   NULL,
    [deferred_revenue_flag]                             CHAR (1)       NULL,
    [department_description]                            CHAR (50)      NULL,
    [department_id]                                     INT            NULL,
    [discount_gl_account]                               CHAR (10)      NULL,
    [display_ui_flag]                                   CHAR (1)       NULL,
    [gl_account_number]                                 CHAR (10)      NULL,
    [gl_department_code]                                CHAR (7)       NULL,
    [gl_over_ride_club_id]                              INT            NULL,
    [gl_product_code]                                   CHAR (10)      NULL,
    [junior_member_dues_flag]                           CHAR (1)       NULL,
    [lt_buck_cost_percent]                              NUMERIC (4, 1) NULL,
    [lt_buck_eligible]                                  BIT            NULL,
    [package_product_flag]                              CHAR (1)       NULL,
    [pay_component]                                     CHAR (50)      NULL,
    [price_locked_flag]                                 CHAR (1)       NULL,
    [product_description]                               CHAR (50)      NULL,
    [product_name]                                      CHAR (20)      NULL,
    [product_status]                                    CHAR (50)      NULL,
    [recurrent_product_type_description]                CHAR (50)      NULL,
    [refund_gl_account_number]                          CHAR (10)      NULL,
    [revenue_category]                                  CHAR (7)       NULL,
    [sales_quantity_factor]                             INT            NULL,
    [sku]                                               VARCHAR (50)   NULL,
    [spend_category]                                    CHAR (7)       NULL,
    [tip_allowed_flag]                                  CHAR (1)       NULL,
    [workday_account]                                   CHAR (6)       NULL,
    [workday_cost_center]                               CHAR (6)       NULL,
    [workday_discount_gl_account]                       CHAR (10)      NULL,
    [workday_offering]                                  CHAR (10)      NULL,
    [workday_over_ride_region]                          CHAR (4)       NULL,
    [workday_refund_gl_account]                         CHAR (10)      NULL,
    [workday_revenue_product_group_account]             CHAR (6)       NULL,
    [workday_revenue_product_group_discount_gl_account] CHAR (10)      NULL,
    [workday_revenue_product_group_refund_gl_account]   CHAR (10)      NULL,
    [dv_load_date_time]                                 DATETIME       NULL,
    [dv_load_end_date_time]                             DATETIME       NULL,
    [dv_batch_id]                                       BIGINT         NOT NULL,
    [dv_inserted_date_time]                             DATETIME       NOT NULL,
    [dv_insert_user]                                    VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                              DATETIME       NULL,
    [dv_update_user]                                    VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_mms_product_key]));

