CREATE TABLE [dbo].[stage_mms_CardLevelPriceRange] (
    [stage_mms_CardLevelPriceRange_id] BIGINT          NOT NULL,
    [CardLevelPriceRangeID]            INT             NULL,
    [ValCardLevelID]                   INT             NULL,
    [ProductID]                        INT             NULL,
    [StartingPrice]                    DECIMAL (26, 6) NULL,
    [EndingPrice]                      DECIMAL (26, 6) NULL,
    [InsertedDateTime]                 DATETIME        NULL,
    [UpdatedDateTime]                  DATETIME        NULL,
    [dv_batch_id]                      BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

