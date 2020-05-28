CREATE TABLE [dbo].[l_ig_it_trn_order_tender] (
    [l_ig_it_trn_order_tender_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)     NOT NULL,
    [change_tender_id]            INT           NULL,
    [order_hdr_id]                INT           NULL,
    [post_acct_no]                NVARCHAR (32) NULL,
    [sub_tender_id]               INT           NULL,
    [tender_id]                   INT           NULL,
    [tender_seq]                  SMALLINT      NULL,
    [tender_type_id]              INT           NULL,
    [dv_load_date_time]           DATETIME      NOT NULL,
    [dv_r_load_source_id]         BIGINT        NOT NULL,
    [dv_inserted_date_time]       DATETIME      NOT NULL,
    [dv_insert_user]              VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]        DATETIME      NULL,
    [dv_update_user]              VARCHAR (50)  NULL,
    [dv_hash]                     CHAR (32)     NOT NULL,
    [dv_batch_id]                 BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_ig_it_trn_order_tender]
    ON [dbo].[l_ig_it_trn_order_tender]([bk_hash] ASC, [l_ig_it_trn_order_tender_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ig_it_trn_order_tender]([dv_batch_id] ASC);

