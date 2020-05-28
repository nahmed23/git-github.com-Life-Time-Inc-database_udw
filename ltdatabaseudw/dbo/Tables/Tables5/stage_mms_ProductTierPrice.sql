CREATE TABLE [dbo].[stage_mms_ProductTierPrice] (
    [stage_mms_ProductTierPrice_id] BIGINT          NOT NULL,
    [ProductTierPriceID]            INT             NULL,
    [ProductTierID]                 INT             NULL,
    [Price]                         DECIMAL (26, 6) NULL,
    [ValMembershipTypeGroupID]      TINYINT         NULL,
    [InsertedDateTime]              DATETIME        NULL,
    [UpdatedDateTime]               DATETIME        NULL,
    [ValCardLevelID]                INT             NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

