CREATE TABLE [dbo].[s_mms_membership_recurrent_product] (
    [s_mms_membership_recurrent_product_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [membership_recurrent_product_id]       INT             NULL,
    [activation_date]                       DATETIME        NULL,
    [cancellation_request_date]             DATETIME        NULL,
    [termination_date]                      DATETIME        NULL,
    [inserted_date_time]                    DATETIME        NULL,
    [updated_date_time]                     DATETIME        NULL,
    [price]                                 DECIMAL (26, 6) NULL,
    [created_date_time]                     DATETIME        NULL,
    [utc_created_date_time]                 DATETIME        NULL,
    [created_date_time_zone]                VARCHAR (4)     NULL,
    [last_updated_date_time]                DATETIME        NULL,
    [utc_last_updated_date_time]            DATETIME        NULL,
    [last_updated_date_time_zone]           VARCHAR (4)     NULL,
    [product_assessed_date_time]            DATETIME        NULL,
    [comments]                              VARCHAR (255)   NULL,
    [number_of_sessions]                    INT             NULL,
    [price_per_session]                     NUMERIC (7, 2)  NULL,
    [product_hold_begin_date]               DATETIME        NULL,
    [product_hold_end_date]                 DATETIME        NULL,
    [sold_not_serviced_flag]                BIT             NULL,
    [retail_price]                          DECIMAL (26, 6) NULL,
    [retail_price_per_session]              DECIMAL (26, 6) NULL,
    [promotion_code]                        VARCHAR (50)    NULL,
    [display_only_flag]                     BIT             NULL,
    [dv_load_date_time]                     DATETIME        NOT NULL,
    [dv_batch_id]                           BIGINT          NOT NULL,
    [dv_r_load_source_id]                   BIGINT          NOT NULL,
    [dv_inserted_date_time]                 DATETIME        NOT NULL,
    [dv_insert_user]                        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                  DATETIME        NULL,
    [dv_update_user]                        VARCHAR (50)    NULL,
    [dv_hash]                               CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_membership_recurrent_product]
    ON [dbo].[s_mms_membership_recurrent_product]([bk_hash] ASC, [s_mms_membership_recurrent_product_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_membership_recurrent_product]([dv_batch_id] ASC);

