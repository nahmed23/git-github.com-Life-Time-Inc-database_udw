﻿CREATE TABLE [dbo].[dim_magento_customer] (
    [dim_magento_customer_id]  BIGINT         IDENTITY (1, 1) NOT NULL,
    [city]                     VARCHAR (255)  NULL,
    [company]                  VARCHAR (255)  NULL,
    [country_id]               VARCHAR (255)  NULL,
    [created_at]               DATETIME       NULL,
    [created_dim_date_key]     CHAR (8)       NULL,
    [created_dim_time_key]     CHAR (8)       NULL,
    [customer_id]              INT            NULL,
    [default_billing]          INT            NULL,
    [default_shipping]         INT            NULL,
    [dim_club_key]             VARCHAR (32)   NULL,
    [dim_employee_key]         VARCHAR (32)   NULL,
    [dim_magento_customer_key] VARCHAR (32)   NULL,
    [dim_mms_member_key]       VARCHAR (32)   NULL,
    [dob]                      DATETIME       NULL,
    [dob_dim_date_key]         VARCHAR (32)   NULL,
    [email]                    VARCHAR (255)  NULL,
    [fax]                      VARCHAR (255)  NULL,
    [first_name]               VARCHAR (255)  NULL,
    [gender]                   INT            NULL,
    [group_id]                 INT            NULL,
    [is_active_flag]           CHAR (1)       NULL,
    [last_name]                VARCHAR (255)  NULL,
    [m1_customer_id]           INT            NULL,
    [middle_name]              VARCHAR (255)  NULL,
    [mms_party_id]             VARCHAR (255)  NULL,
    [post_code]                VARCHAR (255)  NULL,
    [prefix]                   VARCHAR (40)   NULL,
    [region]                   VARCHAR (255)  NULL,
    [region_id]                INT            NULL,
    [store_id]                 INT            NULL,
    [street]                   VARCHAR (8000) NULL,
    [suffix]                   VARCHAR (40)   NULL,
    [telephone]                VARCHAR (255)  NULL,
    [updated_at]               DATETIME       NULL,
    [updated_dim_date_key]     CHAR (8)       NULL,
    [updated_dim_time_key]     CHAR (8)       NULL,
    [dv_load_date_time]        DATETIME       NULL,
    [dv_load_end_date_time]    DATETIME       NULL,
    [dv_batch_id]              BIGINT         NOT NULL,
    [dv_inserted_date_time]    DATETIME       NOT NULL,
    [dv_insert_user]           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]     DATETIME       NULL,
    [dv_update_user]           VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_magento_customer_key]));

