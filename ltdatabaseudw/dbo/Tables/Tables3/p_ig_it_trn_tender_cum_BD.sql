CREATE TABLE [dbo].[p_ig_it_trn_tender_cum_BD] (
    [p_ig_it_trn_tender_cum_BD_id]         BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [bus_day_id]                           INT          NULL,
    [check_type_id]                        INT          NULL,
    [meal_period_id]                       INT          NULL,
    [cashier_emp_id]                       INT          NULL,
    [PMS_post_code]                        SMALLINT     NULL,
    [profit_center_id]                     INT          NULL,
    [tax_removed_code]                     SMALLINT     NULL,
    [tender_id]                            INT          NULL,
    [void_type_id]                         INT          NULL,
    [s_ig_it_trn_tender_cum_BD_id]         BIGINT       NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_first_in_key_series]               INT          NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

