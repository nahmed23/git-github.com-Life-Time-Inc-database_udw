CREATE TABLE [dbo].[l_lt_bucks_products] (
    [l_lt_bucks_products_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)     NOT NULL,
    [product_id]             INT           NULL,
    [vendor_id]              NVARCHAR (20) NULL,
    [last_user]              INT           NULL,
    [dv_load_date_time]      DATETIME      NOT NULL,
    [dv_batch_id]            BIGINT        NOT NULL,
    [dv_r_load_source_id]    BIGINT        NOT NULL,
    [dv_inserted_date_time]  DATETIME      NOT NULL,
    [dv_insert_user]         VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]   DATETIME      NULL,
    [dv_update_user]         VARCHAR (50)  NULL,
    [dv_hash]                CHAR (32)     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_lt_bucks_products]
    ON [dbo].[l_lt_bucks_products]([bk_hash] ASC, [l_lt_bucks_products_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_lt_bucks_products]([dv_batch_id] ASC);

