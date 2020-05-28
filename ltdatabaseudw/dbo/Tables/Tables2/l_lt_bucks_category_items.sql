CREATE TABLE [dbo].[l_lt_bucks_category_items] (
    [l_lt_bucks_category_items_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [citem_id]                     INT          NULL,
    [citem_product]                INT          NULL,
    [citem_category]               INT          NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_batch_id]                  BIGINT       NOT NULL,
    [dv_r_load_source_id]          BIGINT       NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_hash]                      CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_lt_bucks_category_items]
    ON [dbo].[l_lt_bucks_category_items]([bk_hash] ASC, [l_lt_bucks_category_items_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_lt_bucks_category_items]([dv_batch_id] ASC);

