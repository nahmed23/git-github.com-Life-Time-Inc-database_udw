CREATE TABLE [dbo].[p_ig_it_trn_order_item] (
    [p_ig_it_trn_order_item_id]            BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [check_seq]                            SMALLINT     NULL,
    [order_hdr_id]                         INT          NULL,
    [l_ig_it_trn_order_item_id]            BIGINT       NULL,
    [s_ig_it_trn_order_item_id]            BIGINT       NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_first_in_key_series]               INT          NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_ig_it_trn_order_item]
    ON [dbo].[p_ig_it_trn_order_item]([bk_hash] ASC, [p_ig_it_trn_order_item_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_ig_it_trn_order_item]([dv_batch_id] ASC);

