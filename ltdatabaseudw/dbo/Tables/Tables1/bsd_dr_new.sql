CREATE TABLE [dbo].[bsd_dr_new] (
    [club_code]                                   VARCHAR (18)     NULL,
    [club_name]                                   VARCHAR (50)     NULL,
    [workday_region]                              VARCHAR (4)      NULL,
    [mmsRegion]                                   VARCHAR (100)    NULL,
    [booking_id]                                  VARCHAR (4000)   NULL,
    [booking_name]                                VARCHAR (4000)   NULL,
    [start_dim_date_key]                          CHAR (8)         NULL,
    [workday_cost_center]                         CHAR (6)         NULL,
    [workday_offering]                            CHAR (10)        NULL,
    [exerp_product_name]                          VARCHAR (4000)   NULL,
    [mms_product_description]                     CHAR (50)        NULL,
    [dim_exerp_subscription_key]                  CHAR (32)        NULL,
    [subscription_id]                             VARCHAR (4000)   NULL,
    [revenue_recognized_this_period]              DECIMAL (37, 17) NOT NULL,
    [revenue_recognized_this_period_less_ltbucks] DECIMAL (38, 17) NOT NULL,
    [outstanding_booking_revenue]                 DECIMAL (38, 17) NOT NULL,
    [participation_id]                            VARCHAR (4000)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

