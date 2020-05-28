CREATE EXTERNAL TABLE [dbo].[Informatica_610b4fa3_ffde_491b_8b27_6e55cf53e949Ext] (
    [stage_mms_MembershipModificationRequest_id] BIGINT NULL,
    [MembershipModificationRequestID] INT NULL,
    [MembershipID] INT NULL,
    [MemberID] INT NULL,
    [RequestDateTime] DATETIME NULL,
    [UTCRequestDateTime] DATETIME NULL,
    [RequestDateTimeZone] VARCHAR (4) NULL,
    [EffectiveDate] DATETIME NULL,
    [ValMembershipModificationRequestTypeID] INT NULL,
    [ValFlexReasonID] INT NULL,
    [MembershipTypeID] INT NULL,
    [InsertedDateTime] DATETIME NULL,
    [UpdatedDateTime] DATETIME NULL,
    [ValMembershipModificationRequestStatusID] INT NULL,
    [StatusChangedDateTime] DATETIME NULL,
    [EmployeeID] INT NULL,
    [LastEFTMonth] VARCHAR (9) NULL,
    [FutureMembershipUpgradeFlag] BIT NULL,
    [ValMembershipUpgradeDateRangeID] INT NULL,
    [ClubID] INT NULL,
    [CommisionedEmployeeID] INT NULL,
    [FirstMonthsDues] DECIMAL (26, 6) NULL,
    [TotalMonthlyAmount] DECIMAL (26, 6) NULL,
    [MemberAgreementStagingID] INT NULL,
    [MembershipUpgradeMonthYear] DATETIME NULL,
    [AgreementPrice] DECIMAL (26, 6) NULL,
    [WaiveServiceFeeFlag] BIT NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_610b4fa3_ffde_491b_8b27_6e55cf53e949DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_610b4fa3_ffde_491b_8b27_6e55cf53e949',
    FILE_FORMAT = [Informatica_610b4fa3_ffde_491b_8b27_6e55cf53e949FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

