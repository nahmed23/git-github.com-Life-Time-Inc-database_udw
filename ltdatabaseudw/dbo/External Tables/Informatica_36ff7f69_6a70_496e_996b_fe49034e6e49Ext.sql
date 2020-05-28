CREATE EXTERNAL TABLE [dbo].[Informatica_36ff7f69_6a70_496e_996b_fe49034e6e49Ext] (
    [stage_mms_Package_id] BIGINT NULL,
    [PackageID] INT NULL,
    [MemberID] INT NULL,
    [MembershipID] INT NULL,
    [ClubID] INT NULL,
    [NumberOfSessions] SMALLINT NULL,
    [PricePerSession] NUMERIC (9, 4) NULL,
    [EmployeeID] INT NULL,
    [ValPackageStatusID] SMALLINT NULL,
    [MMSTranID] INT NULL,
    [ProductID] INT NULL,
    [TranItemID] INT NULL,
    [CreatedDateTime] DATETIME NULL,
    [UTCCreatedDateTime] DATETIME NULL,
    [CreatedDateTimeZone] VARCHAR (4) NULL,
    [SessionsLeft] SMALLINT NULL,
    [BalanceAmount] NUMERIC (9, 4) NULL,
    [InsertedDateTime] DATETIME NULL,
    [UpdatedDateTime] DATETIME NULL,
    [PackageEditedFlag] BIT NULL,
    [PackageEditDateTime] DATETIME NULL,
    [UTCPackageEditDateTime] DATETIME NULL,
    [PackageEditDateTimeZone] VARCHAR (4) NULL,
    [ExpirationDateTime] DATETIME NULL,
    [UnexpireCount] INT NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_36ff7f69_6a70_496e_996b_fe49034e6e49DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_36ff7f69_6a70_496e_996b_fe49034e6e49',
    FILE_FORMAT = [Informatica_36ff7f69_6a70_496e_996b_fe49034e6e49FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

