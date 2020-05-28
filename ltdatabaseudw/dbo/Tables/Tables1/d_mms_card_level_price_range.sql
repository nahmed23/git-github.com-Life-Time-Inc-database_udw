﻿CREATE TABLE [dbo].[d_mms_card_level_price_range] (
    [d_mms_card_level_price_range_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [card_level_price_range_id]       INT             NULL,
    [card_level_dim_description_key]  VARCHAR (255)   NULL,
    [dim_mms_product_key]             VARCHAR (32)    NULL,
    [ending_price]                    DECIMAL (26, 6) NULL,
    [inserted_date_time]              DATETIME        NULL,
    [inserted_dim_date_key]           VARCHAR (8)     NULL,
    [inserted_dim_time_key]           INT             NULL,
    [product_id]                      INT             NULL,
    [starting_price]                  DECIMAL (26, 6) NULL,
    [updated_date_time]               DATETIME        NULL,
    [updated_dim_date_key]            VARCHAR (8)     NULL,
    [updated_dim_time_key]            INT             NULL,
    [val_card_level_id]               INT             NULL,
    [p_mms_card_level_price_range_id] BIGINT          NOT NULL,
    [deleted_flag]                    INT             NULL,
    [dv_load_date_time]               DATETIME        NULL,
    [dv_load_end_date_time]           DATETIME        NULL,
    [dv_batch_id]                     BIGINT          NOT NULL,
    [dv_inserted_date_time]           DATETIME        NOT NULL,
    [dv_insert_user]                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

