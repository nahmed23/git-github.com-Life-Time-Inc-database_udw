CREATE TABLE [dbo].[stage_mms_MembershipBalanceSnapshot_history] (
    [MMSMembershipBalanceSnapshotKey]    INT          NOT NULL,
    [MembershipBalanceID]                INT          NOT NULL,
    [MembershipID]                       INT          NULL,
    [CurrentBalance]                     MONEY        NULL,
    [EFTAmount]                          MONEY        NULL,
    [StatementBalance]                   MONEY        NULL,
    [AssessedDateTime]                   DATETIME     NULL,
    [StatementDateTime]                  DATETIME     NULL,
    [PreviousStatementBalance]           MONEY        NULL,
    [PreviousStatementDateTime]          DATETIME     NULL,
    [CommittedBalance]                   MONEY        NULL,
    [MMSInsertedDateTime]                DATETIME     NULL,
    [MMSUpdatedDateTime]                 DATETIME     NULL,
    [ResubmitCollectFromBankAccountFlag] BIT          NULL,
    [InsertedDateTime]                   DATETIME     NOT NULL,
    [InsertUser]                         VARCHAR (50) NOT NULL,
    [BatchID]                            INT          NOT NULL,
    [ETLSourceSystemKey]                 INT          NOT NULL,
    [UpdatedDateTime]                    DATETIME     NULL,
    [UpdateUser]                         VARCHAR (50) NULL,
    [CommittedBalanceProducts]           MONEY        NULL,
    [CurrentBalanceProducts]             MONEY        NULL,
    [EFTAmountProducts]                  MONEY        NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

