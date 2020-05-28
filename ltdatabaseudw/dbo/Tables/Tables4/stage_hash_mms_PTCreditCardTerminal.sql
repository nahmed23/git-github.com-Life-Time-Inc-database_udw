CREATE TABLE [dbo].[stage_hash_mms_PTCreditCardTerminal] (
    [stage_hash_mms_PTCreditCardTerminal_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)    NOT NULL,
    [PTCreditCardTerminalID]                 INT          NULL,
    [Name]                                   VARCHAR (15) NULL,
    [Description]                            VARCHAR (50) NULL,
    [ValPTCreditCardClientNumberID]          SMALLINT     NULL,
    [MerchantNumber]                         BIGINT       NULL,
    [TerminalNumber]                         INT          NULL,
    [ClubID]                                 INT          NULL,
    [ValCreditCardTerminalLocationID]        SMALLINT     NULL,
    [InsertedDateTime]                       DATETIME     NULL,
    [UpdatedDateTime]                        DATETIME     NULL,
    [DrawerID]                               INT          NULL,
    [TerminalAreaID]                         INT          NULL,
    [TerminalStatus]                         BIT          NULL,
    [EmployeeID]                             INT          NULL,
    [dv_load_date_time]                      DATETIME     NOT NULL,
    [dv_inserted_date_time]                  DATETIME     NOT NULL,
    [dv_insert_user]                         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                   DATETIME     NULL,
    [dv_update_user]                         VARCHAR (50) NULL,
    [dv_batch_id]                            BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

