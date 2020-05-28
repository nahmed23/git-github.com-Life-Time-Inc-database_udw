﻿CREATE TABLE [dbo].[stage_hash_boss_mbr_phones] (
    [stage_hash_boss_mbr_phones_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)    NOT NULL,
    [id]                            INT          NULL,
    [area_code]                     VARCHAR (3)  NULL,
    [number]                        VARCHAR (7)  NULL,
    [ext]                           VARCHAR (5)  NULL,
    [ph_type]                       VARCHAR (1)  NULL,
    [contact_id]                    INT          NULL,
    [created_at]                    DATETIME     NULL,
    [updated_at]                    DATETIME     NULL,
    [dv_load_date_time]             DATETIME     NOT NULL,
    [dv_inserted_date_time]         DATETIME     NOT NULL,
    [dv_insert_user]                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]          DATETIME     NULL,
    [dv_update_user]                VARCHAR (50) NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

