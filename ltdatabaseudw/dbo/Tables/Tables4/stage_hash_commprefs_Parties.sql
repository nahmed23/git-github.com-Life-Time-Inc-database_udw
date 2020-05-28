﻿CREATE TABLE [dbo].[stage_hash_commprefs_Parties] (
    [stage_hash_commprefs_Parties_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)    NOT NULL,
    [Id]                              INT          NULL,
    [CreatedTime]                     DATETIME     NULL,
    [UpdatedTime]                     DATETIME     NULL,
    [dv_load_date_time]               DATETIME     NOT NULL,
    [dv_inserted_date_time]           DATETIME     NOT NULL,
    [dv_insert_user]                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]            DATETIME     NULL,
    [dv_update_user]                  VARCHAR (50) NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

