﻿CREATE TABLE [dbo].[l_mms_product_tier_price_1] (
    [l_mms_product_tier_price_1_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [product_tier_price_id]         INT          NULL,
    [val_card_level_id]             INT          NULL,
    [dv_load_date_time]             DATETIME     NOT NULL,
    [dv_r_load_source_id]           BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL,
    [dv_hash]                       CHAR (32)    NOT NULL,
    [dv_deleted]                    BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

