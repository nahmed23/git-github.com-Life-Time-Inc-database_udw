CREATE EXTERNAL TABLE [dbo].[Informatica_63c351bd_4aa7_4854_956d_7400bef321b9Ext] (
    [stage_mms_ValMembershipSource_id] BIGINT NULL,
    [ValMembershipSourceID] INT NULL,
    [Description] VARCHAR (50) NULL,
    [SortOrder] INT NULL,
    [InsertedDateTime] DATETIME NULL,
    [UpdatedDateTime] DATETIME NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_63c351bd_4aa7_4854_956d_7400bef321b9DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_63c351bd_4aa7_4854_956d_7400bef321b9',
    FILE_FORMAT = [Informatica_63c351bd_4aa7_4854_956d_7400bef321b9FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

