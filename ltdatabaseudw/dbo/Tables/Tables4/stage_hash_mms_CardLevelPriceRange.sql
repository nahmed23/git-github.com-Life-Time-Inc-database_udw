﻿CREATE TABLE [dbo].[stage_hash_mms_CardLevelPriceRange] (
    [stage_hash_mms_CardLevelPriceRange_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [CardLevelPriceRangeID]                 INT             NULL,
    [ValCardLevelID]                        INT             NULL,
    [ProductID]                             INT             NULL,
    [StartingPrice]                         DECIMAL (26, 6) NULL,
    [EndingPrice]                           DECIMAL (26, 6) NULL,
    [InsertedDateTime]                      DATETIME        NULL,
    [UpdatedDateTime]                       DATETIME        NULL,
    [dv_load_date_time]                     DATETIME        NOT NULL,
    [dv_batch_id]                           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

