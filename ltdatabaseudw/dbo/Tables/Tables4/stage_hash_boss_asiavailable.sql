﻿CREATE TABLE [dbo].[stage_hash_boss_asiavailable] (
    [stage_hash_boss_asiavailable_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [club]                            INT          NULL,
    [resource_id]                     INT          NULL,
    [start_time]                      DATETIME     NULL,
    [end_time]                        DATETIME     NULL,
    [schedule_type]                   CHAR (1)     NULL,
    [dv_load_date_time]               DATETIME     NOT NULL,
    [dv_updated_date_time]            DATETIME     NULL,
    [dv_update_user]                  VARCHAR (50) NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

