﻿CREATE TABLE [dbo].[d_magento_customer_entity] (
    [d_magento_customer_entity_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)     NOT NULL,
    [dim_magento_customer_key]     VARCHAR (32)  NULL,
    [customer_id]                  INT           NULL,
    [confirmation]                 VARCHAR (64)  NULL,
    [created_at]                   DATETIME      NULL,
    [created_dim_date_key]         CHAR (8)      NULL,
    [created_dim_time_key]         CHAR (8)      NULL,
    [created_in]                   VARCHAR (255) NULL,
    [default_billing]              INT           NULL,
    [default_shipping]             INT           NULL,
    [dob]                          DATETIME      NULL,
    [dob_dim_date_key]             CHAR (8)      NULL,
    [email]                        VARCHAR (255) NULL,
    [failures_num]                 INT           NULL,
    [first_failure_dim_date_key]   CHAR (8)      NULL,
    [first_failure_dim_time_key]   CHAR (8)      NULL,
    [first_name]                   VARCHAR (255) NULL,
    [gender]                       INT           NULL,
    [group_id]                     INT           NULL,
    [increment_id]                 VARCHAR (50)  NULL,
    [is_active_flag]               CHAR (1)      NULL,
    [last_name]                    VARCHAR (255) NULL,
    [lock_expires_dim_date_key]    CHAR (8)      NULL,
    [lock_expires_dim_time_key]    CHAR (8)      NULL,
    [m1_customer_id]               INT           NULL,
    [middle_name]                  VARCHAR (255) NULL,
    [prefix]                       VARCHAR (40)  NULL,
    [store_id]                     INT           NULL,
    [suffix]                       VARCHAR (40)  NULL,
    [tax_vat]                      VARCHAR (50)  NULL,
    [updated_at]                   DATETIME      NULL,
    [updated_dim_date_key]         CHAR (8)      NULL,
    [updated_dim_time_key]         CHAR (8)      NULL,
    [website_id]                   INT           NULL,
    [p_magento_customer_entity_id] BIGINT        NOT NULL,
    [deleted_flag]                 INT           NULL,
    [dv_load_date_time]            DATETIME      NULL,
    [dv_load_end_date_time]        DATETIME      NULL,
    [dv_batch_id]                  BIGINT        NOT NULL,
    [dv_inserted_date_time]        DATETIME      NOT NULL,
    [dv_insert_user]               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]         DATETIME      NULL,
    [dv_update_user]               VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

