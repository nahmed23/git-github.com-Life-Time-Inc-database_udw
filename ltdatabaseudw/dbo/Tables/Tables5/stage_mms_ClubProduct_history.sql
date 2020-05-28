CREATE TABLE [dbo].[stage_mms_ClubProduct_history] (
    [MMSClubProductKey]   INT          NULL,
    [ClubProductID]       INT          NULL,
    [ClubID]              INT          NULL,
    [ProductID]           INT          NULL,
    [Price]               MONEY        NULL,
    [ValCommissionableID] TINYINT      NULL,
    [MMSInsertedDateTime] DATETIME     NULL,
    [SoldInPK]            BIT          NULL,
    [MMSUpdatedDateTime]  DATETIME     NULL,
    [InsertedDateTime]    DATETIME     NULL,
    [InsertUser]          VARCHAR (50) NULL,
    [BatchID]             INT          NULL,
    [ETLSourceSystemKey]  INT          NULL,
    [DeletedInd]          CHAR (1)     NULL,
    [UpdatedDateTime]     DATETIME     NULL,
    [UpdateUser]          VARCHAR (50) NULL,
    [OriginalClubID]      INT          NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

