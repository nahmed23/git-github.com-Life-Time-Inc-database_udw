CREATE TABLE [dbo].[stage_mms_MembershipProductTier_history] (
    [MembershipProductTierKey] BIGINT       NOT NULL,
    [MembershipProductTierID]  INT          NULL,
    [MembershipID]             INT          NULL,
    [ProductTierID]            INT          NULL,
    [MMSInsertedDateTime]      DATETIME     NULL,
    [MMSUpdatedDateTime]       DATETIME     NULL,
    [LastUpdatedEmployeeID]    INT          NULL,
    [InsertedDateTime]         DATETIME     NOT NULL,
    [InsertUser]               VARCHAR (50) NULL,
    [UpdatedDateTime]          DATETIME     NULL,
    [UpdateUser]               VARCHAR (50) NULL,
    [BatchID]                  INT          NOT NULL,
    [ETLSourceSystemKey]       INT          NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

