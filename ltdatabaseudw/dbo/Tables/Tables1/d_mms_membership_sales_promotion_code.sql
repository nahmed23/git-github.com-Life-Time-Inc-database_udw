﻿CREATE TABLE [dbo].[d_mms_membership_sales_promotion_code] (
    [d_mms_membership_sales_promotion_code_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)    NOT NULL,
    [membership_sales_promotion_code_id]       INT          NULL,
    [dim_mms_member_key]                       VARCHAR (32) NULL,
    [dim_mms_membership_key]                   VARCHAR (32) NULL,
    [dim_mms_sales_promotion_code_key]         VARCHAR (32) NULL,
    [inserted_dim_date_key]                    VARCHAR (8)  NULL,
    [inserted_dim_time_key]                    INT          NULL,
    [member_id]                                INT          NULL,
    [membership_id]                            INT          NULL,
    [sales_advisor_dim_employee_key]           VARCHAR (32) NULL,
    [sales_advisor_employee_id]                INT          NULL,
    [sales_promotion_code_id]                  INT          NULL,
    [updated_dim_date_key]                     VARCHAR (8)  NULL,
    [updated_dim_time_key]                     INT          NULL,
    [p_mms_membership_sales_promotion_code_id] BIGINT       NOT NULL,
    [deleted_flag]                             INT          NULL,
    [dv_load_date_time]                        DATETIME     NULL,
    [dv_load_end_date_time]                    DATETIME     NULL,
    [dv_batch_id]                              BIGINT       NOT NULL,
    [dv_inserted_date_time]                    DATETIME     NOT NULL,
    [dv_insert_user]                           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                     DATETIME     NULL,
    [dv_update_user]                           VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

