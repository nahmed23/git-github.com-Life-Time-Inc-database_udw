CREATE TABLE [dbo].[l_ig_it_trn_order_discount] (
    [l_ig_it_trn_order_discount_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [check_seq]                     SMALLINT     NULL,
    [discoup_id]                    INT          NULL,
    [emp_id]                        INT          NULL,
    [order_hdr_id]                  INT          NULL,
    [dv_load_date_time]             DATETIME     NOT NULL,
    [dv_r_load_source_id]           BIGINT       NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL,
    [dv_hash]                       CHAR (32)    NOT NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_ig_it_trn_order_discount]
    ON [dbo].[l_ig_it_trn_order_discount]([bk_hash] ASC, [l_ig_it_trn_order_discount_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ig_it_trn_order_discount]([dv_batch_id] ASC);

