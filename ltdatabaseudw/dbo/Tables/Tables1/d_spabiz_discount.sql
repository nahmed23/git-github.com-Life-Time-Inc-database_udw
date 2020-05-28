CREATE TABLE [dbo].[d_spabiz_discount] (
    [d_spabiz_discount_id]                  BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [dim_spabiz_discount_key]               CHAR (32)       NULL,
    [discount_id]                           BIGINT          NULL,
    [store_number]                          BIGINT          NULL,
    [all_retail_commission_flag]            CHAR (1)        NULL,
    [all_service_commission_flag]           CHAR (1)        NULL,
    [amount]                                DECIMAL (26, 6) NULL,
    [apply_to_dim_description_key]          VARCHAR (50)    NULL,
    [apply_to_id]                           VARCHAR (50)    NULL,
    [apply_when_dim_description_key]        VARCHAR (50)    NULL,
    [apply_when_id]                         VARCHAR (50)    NULL,
    [associated_with_promotion_flag]        CHAR (1)        NULL,
    [deleted_date_time]                     DATETIME        NULL,
    [deleted_flag]                          CHAR (1)        NULL,
    [dim_spabiz_store_key]                  CHAR (32)       NULL,
    [discount_category_dim_description_key] VARCHAR (50)    NULL,
    [discount_category_id]                  VARCHAR (50)    NULL,
    [edit_date_time]                        DATETIME        NULL,
    [from_date_time]                        DATETIME        NULL,
    [name]                                  VARCHAR (180)   NULL,
    [pay_commission_flag]                   CHAR (1)        NULL,
    [pay_retail_commission_flag]            CHAR (1)        NULL,
    [pay_service_commission_flag]           CHAR (1)        NULL,
    [percent_dollar_dim_description_key]    VARCHAR (50)    NULL,
    [percent_dollar_id]                     VARCHAR (50)    NULL,
    [quick_id]                              VARCHAR (45)    NULL,
    [to_date_time]                          DATETIME        NULL,
    [use_date_range_flag]                   CHAR (1)        NULL,
    [p_spabiz_discount_id]                  BIGINT          NOT NULL,
    [dv_load_date_time]                     DATETIME        NULL,
    [dv_load_end_date_time]                 DATETIME        NULL,
    [dv_batch_id]                           BIGINT          NOT NULL,
    [dv_inserted_date_time]                 DATETIME        NOT NULL,
    [dv_insert_user]                        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                  DATETIME        NULL,
    [dv_update_user]                        VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_discount]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_discount]([dv_batch_id]);

