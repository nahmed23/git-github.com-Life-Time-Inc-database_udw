﻿CREATE TABLE [dbo].[stage_mms_ValNameSuffix] (
    [stage_mms_ValNameSuffix_id] BIGINT       NOT NULL,
    [ValNameSuffixID]            INT          NULL,
    [Description]                VARCHAR (50) NULL,
    [SortOrder]                  INT          NULL,
    [InsertedDateTime]           DATETIME     NULL,
    [UpdatedDateTime]            DATETIME     NULL,
    [dv_batch_id]                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

