CREATE TABLE [dbo].[s_mms_product_2] (
    [s_mms_product_2_id]         BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)    NOT NULL,
    [product_id]                 INT          NULL,
    [exclude_from_club_POS_flag] BIT          NULL,
    [access_by_price_paid_flag]  BIT          NULL,
    [dv_load_date_time]          DATETIME     NOT NULL,
    [dv_r_load_source_id]        BIGINT       NOT NULL,
    [dv_inserted_date_time]      DATETIME     NOT NULL,
    [dv_insert_user]             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]       DATETIME     NULL,
    [dv_update_user]             VARCHAR (50) NULL,
    [dv_hash]                    CHAR (32)    NOT NULL,
    [dv_deleted]                 BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

