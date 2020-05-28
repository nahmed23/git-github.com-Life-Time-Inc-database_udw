CREATE EXTERNAL TABLE [dbo].[Informatica_417274c3_def1_4afc_89f1_4622e5db5fa0Ext] (
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
    DATA_SOURCE = [Informatica_417274c3_def1_4afc_89f1_4622e5db5fa0DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_417274c3_def1_4afc_89f1_4622e5db5fa0',
    FILE_FORMAT = [Informatica_417274c3_def1_4afc_89f1_4622e5db5fa0FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

