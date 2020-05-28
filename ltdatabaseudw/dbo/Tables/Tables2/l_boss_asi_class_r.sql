﻿CREATE TABLE [dbo].[l_boss_asi_class_r] (
    [l_boss_asi_class_r_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [class_r_dept]          INT          NULL,
    [class_r_class]         INT          NULL,
    [class_r_tax_code]      CHAR (1)     NULL,
    [class_r_interest_id]   INT          NULL,
    [class_r_id]            INT          NULL,
    [class_r_format_id]     INT          NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_boss_asi_class_r]
    ON [dbo].[l_boss_asi_class_r]([bk_hash] ASC, [l_boss_asi_class_r_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_boss_asi_class_r]([dv_batch_id] ASC);

