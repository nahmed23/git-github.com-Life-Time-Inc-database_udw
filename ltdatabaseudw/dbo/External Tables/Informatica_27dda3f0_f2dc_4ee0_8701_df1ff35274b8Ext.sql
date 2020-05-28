CREATE EXTERNAL TABLE [dbo].[Informatica_27dda3f0_f2dc_4ee0_8701_df1ff35274b8Ext] (
    [stage_mms_EFT_id] BIGINT NULL,
    [EFTID] INT NULL,
    [MembershipID] INT NULL,
    [ValEFTStatusID] TINYINT NULL,
    [EFTReturnCodeID] INT NULL,
    [AccountNumber] VARCHAR (50) NULL,
    [AccountOwner] VARCHAR (50) NULL,
    [RoutingNumber] VARCHAR (9) NULL,
    [ExpirationDate] DATETIME NULL,
    [EFTDate] DATETIME NULL,
    [PaymentID] INT NULL,
    [ReturnCode] VARCHAR (10) NULL,
    [ValEFTTypeID] TINYINT NULL,
    [EFTAmount] DECIMAL (26, 6) NULL,
    [ValPaymentTypeID] TINYINT NULL,
    [MemberID] INT NULL,
    [Job_Task_ID] INT NULL,
    [MaskedAccountNumber] VARCHAR (50) NULL,
    [MaskedAccountNumber64] VARCHAR (17) NULL,
    [InsertedDateTime] DATETIME NULL,
    [UpdatedDateTime] DATETIME NULL,
    [DuesAmountUsedForProducts] DECIMAL (26, 6) NULL,
    [EFTAmountProducts] DECIMAL (26, 6) NULL,
    [OrderNumber] VARCHAR (15) NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_27dda3f0_f2dc_4ee0_8701_df1ff35274b8DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_27dda3f0_f2dc_4ee0_8701_df1ff35274b8',
    FILE_FORMAT = [Informatica_27dda3f0_f2dc_4ee0_8701_df1ff35274b8FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

