﻿CREATE TABLE [dbo].[l_lt_bucks_transaction_fifo] (
    [l_lt_bucks_transaction_fifo_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)    NOT NULL,
    [tfifo_id]                       INT          NULL,
    [tfifo_transaction_1]            INT          NULL,
    [tfifo_transaction_2]            INT          NULL,
    [dv_load_date_time]              DATETIME     NOT NULL,
    [dv_batch_id]                    BIGINT       NOT NULL,
    [dv_r_load_source_id]            BIGINT       NOT NULL,
    [dv_inserted_date_time]          DATETIME     NOT NULL,
    [dv_insert_user]                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]           DATETIME     NULL,
    [dv_update_user]                 VARCHAR (50) NULL,
    [dv_hash]                        CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_lt_bucks_transaction_fifo]
    ON [dbo].[l_lt_bucks_transaction_fifo]([bk_hash] ASC, [l_lt_bucks_transaction_fifo_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_lt_bucks_transaction_fifo]([dv_batch_id] ASC);

