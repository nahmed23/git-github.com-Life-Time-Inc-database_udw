﻿CREATE TABLE [dbo].[proposed_fact_exerp_subscription_participation] (
    [club_id]                           INT              NULL,
    [delivered_dim_date_key]            CHAR (8)         NULL,
    [delivered_dim_time_key]            CHAR (8)         NULL,
    [delivered_dim_club_key]            CHAR (32)        NULL,
    [dim_employee_key]                  CHAR (32)        NULL,
    [dim_exerp_booking_key]             VARCHAR (32)     NULL,
    [dim_exerp_product_key]             CHAR (32)        NULL,
    [dim_exerp_subscription_key]        VARCHAR (32)     NULL,
    [dim_exerp_subscription_period_key] CHAR (32)        NULL,
    [dim_mms_member_key]                VARCHAR (32)     NULL,
    [dim_mms_product_key]               CHAR (32)        NULL,
    [employee_id]                       INT              NULL,
    [fact_exerp_participation_key]      VARCHAR (32)     NULL,
    [member_id]                         INT              NULL,
    [pay_period_first_day_dim_date_key] CHAR (8)         NULL,
    [payroll_description]               VARCHAR (500)    NULL,
    [payroll_group_description]         VARCHAR (500)    NULL,
    [payroll_region_type]               VARCHAR (500)    NULL,
    [payroll_service_amount_flag]       CHAR (1)         NULL,
    [payroll_service_quantity_flag]     CHAR (1)         NULL,
    [price_per_booking]                 DECIMAL (37, 17) NULL,
    [price_per_booking_less_lt_bucks]   DECIMAL (38, 17) NULL,
    [product_id]                        INT              NULL,
    [dv_load_date_time]                 VARCHAR (11)     NOT NULL,
    [dv_load_end_date_time]             VARCHAR (12)     NOT NULL,
    [dv_batch_id]                       INT              NOT NULL,
    [dv_inserted_date_time]             DATETIME         NULL,
    [dv_insert_user]                    VARCHAR (50)     NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_exerp_subscription_period_key]));

