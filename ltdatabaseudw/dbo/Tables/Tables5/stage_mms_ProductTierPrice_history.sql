CREATE TABLE [dbo].[stage_mms_ProductTierPrice_history] (
    [MMSProductTierPriceKey]   INT          NOT NULL,
    [ProductTierPriceID]       INT          NOT NULL,
    [ProductTierID]            INT          NOT NULL,
    [Price]                    MONEY        NOT NULL,
    [ValMembershipTypeGroupID] TINYINT      NULL,
    [MMSInsertedDateTime]      DATETIME     NULL,
    [MMSUpdatedDateTime]       DATETIME     NULL,
    [ValCardLevelID]           INT          NULL,
    [InsertedDateTime]         DATETIME     NOT NULL,
    [InsertUser]               VARCHAR (50) NULL,
    [UpdatedDateTime]          DATETIME     NULL,
    [UpdateUser]               VARCHAR (50) NULL,
    [BatchID]                  INT          NOT NULL,
    [ETLSourceSystemKey]       INT          NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

