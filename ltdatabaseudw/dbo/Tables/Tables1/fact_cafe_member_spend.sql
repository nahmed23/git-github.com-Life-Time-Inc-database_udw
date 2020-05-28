CREATE TABLE [dbo].[fact_cafe_member_spend] (
    [fact_cafe_member_spend_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_mms_member_key]         VARCHAR (32)    NULL,
    [dim_mms_membership_key]     VARCHAR (32)    NULL,
    [last_12_month_spend_amount] DECIMAL (26, 6) NULL,
    [total_spend_amount]         DECIMAL (26, 6) NULL,
    [dv_load_date_time]          DATETIME        NULL,
    [dv_load_end_date_time]      DATETIME        NULL,
    [dv_batch_id]                BIGINT          NOT NULL,
    [dv_inserted_date_time]      DATETIME        NOT NULL,
    [dv_insert_user]             VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]       DATETIME        NULL,
    [dv_update_user]             VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

