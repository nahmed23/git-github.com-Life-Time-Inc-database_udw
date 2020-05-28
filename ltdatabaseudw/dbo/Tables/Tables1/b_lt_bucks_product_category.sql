CREATE TABLE [dbo].[b_lt_bucks_product_category] (
    [b_lt_bucks_product_category_id] BIGINT       NOT NULL,
    [citem_id]                       INT          NOT NULL,
    [product_id]                     INT          NOT NULL,
    [category_id]                    INT          NOT NULL,
    [dv_load_date_time]              DATETIME     NOT NULL,
    [dv_load_end_date_time]          DATETIME     NOT NULL,
    [dv_batch_id]                    BIGINT       NOT NULL,
    [dv_r_load_source_id]            BIGINT       NOT NULL,
    [dv_inserted_date_time]          DATETIME     NOT NULL,
    [dv_insert_user]                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]           DATETIME     NULL,
    [dv_update_user]                 VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([b_lt_bucks_product_category_id]));

