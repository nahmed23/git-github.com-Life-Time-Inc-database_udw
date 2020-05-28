CREATE TABLE [dbo].[d_lt_bucks_product_options] (
    [d_lt_bucks_product_options_id]        BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)       NOT NULL,
    [dim_lt_bucks_product_options_key]     CHAR (32)       NULL,
    [poption_id]                           INT             NULL,
    [dim_lt_bucks_product_key]             CHAR (32)       NULL,
    [dim_mms_product_key]                  CHAR (32)       NULL,
    [last_modified_timestamp]              DATETIME        NULL,
    [last_modified_timestamp_dim_date_key] CHAR (8)        NULL,
    [last_modified_timestamp_dim_time_key] CHAR (8)        NULL,
    [mms_multiplier]                       INT             NULL,
    [poption_active_flag]                  CHAR (1)        NULL,
    [poption_expiration_days]              INT             NULL,
    [poption_timestamp]                    DATETIME        NULL,
    [poption_timestamp_dim_date_key]       CHAR (8)        NULL,
    [poption_timestamp_dim_time_key]       CHAR (8)        NULL,
    [price]                                DECIMAL (4)     NULL,
    [product_option_description]           NVARCHAR (2000) NULL,
    [product_option_name]                  NVARCHAR (50)   NULL,
    [deleted_flag]                         INT             NULL,
    [p_lt_bucks_product_options_id]        BIGINT          NOT NULL,
    [dv_load_date_time]                    DATETIME        NULL,
    [dv_load_end_date_time]                DATETIME        NULL,
    [dv_batch_id]                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                DATETIME        NOT NULL,
    [dv_insert_user]                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                 DATETIME        NULL,
    [dv_update_user]                       VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_lt_bucks_product_options]([dv_batch_id] ASC);

