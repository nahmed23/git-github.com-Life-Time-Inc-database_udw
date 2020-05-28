﻿CREATE TABLE [dbo].[d_boss_interest] (
    [d_boss_interest_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [interest_id]           INT          NULL,
    [interest_long_desc]    CHAR (50)    NULL,
    [interest_short_desc]   CHAR (15)    NULL,
    [p_boss_interest_id]    BIGINT       NOT NULL,
    [deleted_flag]          INT          NULL,
    [dv_load_date_time]     DATETIME     NULL,
    [dv_load_end_date_time] DATETIME     NULL,
    [dv_batch_id]           BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_interest]([dv_batch_id] ASC);

