CREATE TABLE [dbo].[d_ig_it_trn_emp_check_cum_BD] (
    [d_ig_it_trn_emp_check_cum_BD_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)    NOT NULL,
    [fact_cafe_employee_check_cumlative_key] VARCHAR (32) NULL,
    [bus_day_id]                             INT          NULL,
    [check_type_id]                          INT          NULL,
    [meal_period_id]                         INT          NULL,
    [profit_center_id]                       INT          NULL,
    [server_emp_id]                          INT          NULL,
    [void_type_id]                           INT          NULL,
    [dim_cafe_business_day_dates_key]        VARCHAR (32) NULL,
    [dim_cafe_check_type_key]                VARCHAR (32) NULL,
    [dim_cafe_meal_period_key]               VARCHAR (32) NULL,
    [dim_cafe_profit_center_key]             VARCHAR (32) NULL,
    [number_checks]                          INT          NULL,
    [number_covers]                          INT          NULL,
    [server_dim_employee_key]                VARCHAR (32) NULL,
    [p_ig_it_trn_emp_check_cum_BD_id]        BIGINT       NOT NULL,
    [deleted_flag]                           INT          NULL,
    [dv_load_date_time]                      DATETIME     NULL,
    [dv_load_end_date_time]                  DATETIME     NULL,
    [dv_batch_id]                            BIGINT       NOT NULL,
    [dv_inserted_date_time]                  DATETIME     NOT NULL,
    [dv_insert_user]                         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                   DATETIME     NULL,
    [dv_update_user]                         VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

