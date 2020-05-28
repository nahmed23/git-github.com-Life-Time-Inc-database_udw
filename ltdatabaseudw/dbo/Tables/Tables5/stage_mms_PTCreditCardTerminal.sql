CREATE TABLE [dbo].[stage_mms_PTCreditCardTerminal] (
    [stage_mms_PTCreditCardTerminal_id] BIGINT       NOT NULL,
    [PTCreditCardTerminalID]            INT          NULL,
    [Name]                              VARCHAR (15) NULL,
    [Description]                       VARCHAR (50) NULL,
    [ValPTCreditCardClientNumberID]     SMALLINT     NULL,
    [MerchantNumber]                    BIGINT       NULL,
    [TerminalNumber]                    INT          NULL,
    [ClubID]                            INT          NULL,
    [ValCreditCardTerminalLocationID]   SMALLINT     NULL,
    [InsertedDateTime]                  DATETIME     NULL,
    [UpdatedDateTime]                   DATETIME     NULL,
    [DrawerID]                          INT          NULL,
    [TerminalAreaID]                    INT          NULL,
    [TerminalStatus]                    BIT          NULL,
    [EmployeeID]                        INT          NULL,
    [dv_batch_id]                       BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

