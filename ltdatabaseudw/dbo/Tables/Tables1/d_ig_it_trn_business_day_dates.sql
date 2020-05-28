CREATE TABLE [dbo].[d_ig_it_trn_business_day_dates] (
    [d_ig_it_trn_business_day_dates_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)    NOT NULL,
    [dim_cafe_business_day_dates_key]   VARCHAR (32) NULL,
    [bus_day_id]                        INT          NULL,
    [business_day_end_dim_date_key]     VARCHAR (8)  NULL,
    [business_day_end_dim_time_key]     INT          NULL,
    [business_day_start_dim_date_key]   VARCHAR (8)  NULL,
    [business_day_start_dim_time_key]   INT          NULL,
    [p_ig_it_trn_business_day_dates_id] BIGINT       NOT NULL,
    [deleted_flag]                      INT          NULL,
    [dv_load_date_time]                 DATETIME     NULL,
    [dv_load_end_date_time]             DATETIME     NULL,
    [dv_batch_id]                       BIGINT       NOT NULL,
    [dv_inserted_date_time]             DATETIME     NOT NULL,
    [dv_insert_user]                    VARCHAR (50) NOT NULL,
    [dv_updated_date_time]              DATETIME     NULL,
    [dv_update_user]                    VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

