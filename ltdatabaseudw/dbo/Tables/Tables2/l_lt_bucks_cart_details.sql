CREATE TABLE [dbo].[l_lt_bucks_cart_details] (
    [l_lt_bucks_cart_details_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)    NOT NULL,
    [cdetail_id]                 INT          NULL,
    [cdetail_cart]               INT          NULL,
    [cdetail_poption]            INT          NULL,
    [cdetail_club]               INT          NULL,
    [cdetail_transaction_key]    INT          NULL,
    [cdetail_package]            INT          NULL,
    [cdetail_assembly_cart]      INT          NULL,
    [cdetail_qty_expand_cart]    INT          NULL,
    [dv_load_date_time]          DATETIME     NOT NULL,
    [dv_r_load_source_id]        BIGINT       NOT NULL,
    [dv_inserted_date_time]      DATETIME     NOT NULL,
    [dv_insert_user]             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]       DATETIME     NULL,
    [dv_update_user]             VARCHAR (50) NULL,
    [dv_hash]                    CHAR (32)    NOT NULL,
    [dv_batch_id]                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_lt_bucks_cart_details]
    ON [dbo].[l_lt_bucks_cart_details]([bk_hash] ASC, [l_lt_bucks_cart_details_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_lt_bucks_cart_details]([dv_batch_id] ASC);

