CREATE TABLE [dbo].[p_hybris_seven_day_orders] (
    [p_hybris_seven_day_orders_id]         BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)      NOT NULL,
    [order_code]                           NVARCHAR (255) NULL,
    [order_entry_entry_number]             INT            NULL,
    [l_hybris_seven_day_orders_id]         BIGINT         NULL,
    [s_hybris_seven_day_orders_id]         BIGINT         NULL,
    [dv_load_date_time]                    DATETIME       NOT NULL,
    [dv_load_end_date_time]                DATETIME       NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME       NULL,
    [dv_next_greatest_satellite_date_time] DATETIME       NULL,
    [dv_first_in_key_series]               INT            NULL,
    [dv_inserted_date_time]                DATETIME       NOT NULL,
    [dv_insert_user]                       VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                 DATETIME       NULL,
    [dv_update_user]                       VARCHAR (50)   NULL,
    [dv_batch_id]                          BIGINT         NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_hybris_seven_day_orders]([dv_batch_id] ASC);

