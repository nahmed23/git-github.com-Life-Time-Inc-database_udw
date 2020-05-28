CREATE EXTERNAL TABLE [dbo].[Informatica_d0696a67_a3c7_4327_9ce5_539aac134b3bExt] (
    [stage_mms_MembershipAttribute_id] BIGINT NULL,
    [MembershipAttributeID] INT NULL,
    [MembershipID] INT NULL,
    [AttributeValue] VARCHAR (50) NULL,
    [ValMembershipAttributeTypeID] INT NULL,
    [InsertedDateTime] DATETIME NULL,
    [UpdatedDateTime] DATETIME NULL,
    [EffectiveFromDateTime] DATETIME NULL,
    [EffectiveThruDateTime] DATETIME NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_d0696a67_a3c7_4327_9ce5_539aac134b3bDS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_d0696a67_a3c7_4327_9ce5_539aac134b3b',
    FILE_FORMAT = [Informatica_d0696a67_a3c7_4327_9ce5_539aac134b3bFF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

