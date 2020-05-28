CREATE TABLE [dbo].[stage_hash_mms_MembershipBalanceSnapshot] (
    [stage_hash_mms_MembershipBalanceSnapshot_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)       NOT NULL,
    [MembershipBalanceID]                         INT             NULL,
    [MembershipID]                                INT             NULL,
    [CurrentBalance]                              DECIMAL (26, 6) NULL,
    [EFTAmount]                                   DECIMAL (26, 6) NULL,
    [StatementBalance]                            DECIMAL (26, 6) NULL,
    [AssessedDateTime]                            DATETIME        NULL,
    [StatementDateTime]                           DATETIME        NULL,
    [PreviousStatementBalance]                    DECIMAL (26, 6) NULL,
    [PreviousStatementDateTime]                   DATETIME        NULL,
    [CommittedBalance]                            DECIMAL (26, 6) NULL,
    [InsertedDateTime]                            DATETIME        NULL,
    [UpdatedDateTime]                             DATETIME        NULL,
    [ResubmitCollectFromBankAccountFlag]          BIT             NULL,
    [CommittedBalanceProducts]                    DECIMAL (26, 6) NULL,
    [CurrentBalanceProducts]                      DECIMAL (26, 6) NULL,
    [EFTAmountProducts]                           DECIMAL (26, 6) NULL,
    [dv_load_date_time]                           DATETIME        NOT NULL,
    [dv_inserted_date_time]                       DATETIME        NOT NULL,
    [dv_insert_user]                              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                        DATETIME        NULL,
    [dv_update_user]                              VARCHAR (50)    NULL,
    [dv_batch_id]                                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

