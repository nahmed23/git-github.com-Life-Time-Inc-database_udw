CREATE TABLE [dbo].[l_ig_it_trn_order_item] (
    [l_ig_it_trn_order_item_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)    NOT NULL,
    [check_seq]                 SMALLINT     NULL,
    [check_type_id]             INT          NULL,
    [check_void_reason_id]      INT          NULL,
    [discoup_id]                INT          NULL,
    [meal_period_id]            INT          NULL,
    [menu_item_id]              INT          NULL,
    [order_hdr_id]              INT          NULL,
    [price_level_id]            INT          NULL,
    [profit_center_id]          INT          NULL,
    [server_emp_id]             INT          NULL,
    [void_reason_id]            INT          NULL,
    [void_type_id]              INT          NULL,
    [package_id]                INT          NULL,
    [dv_load_date_time]         DATETIME     NOT NULL,
    [dv_r_load_source_id]       BIGINT       NOT NULL,
    [dv_inserted_date_time]     DATETIME     NOT NULL,
    [dv_insert_user]            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]      DATETIME     NULL,
    [dv_update_user]            VARCHAR (50) NULL,
    [dv_hash]                   CHAR (32)    NOT NULL,
    [dv_batch_id]               BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_ig_it_trn_order_item]
    ON [dbo].[l_ig_it_trn_order_item]([bk_hash] ASC, [l_ig_it_trn_order_item_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ig_it_trn_order_item]([dv_batch_id] ASC);

