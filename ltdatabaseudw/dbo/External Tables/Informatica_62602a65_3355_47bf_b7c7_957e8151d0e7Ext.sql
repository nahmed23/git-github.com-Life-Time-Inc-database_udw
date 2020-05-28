CREATE EXTERNAL TABLE [dbo].[Informatica_62602a65_3355_47bf_b7c7_957e8151d0e7Ext] (
    [stage_mms_MembershipAudit_id] BIGINT NULL,
    [MembershipAuditId] INT NULL,
    [RowId] INT NULL,
    [Operation] VARCHAR (10) NULL,
    [ModifiedDateTime] DATETIME NULL,
    [ModifiedUser] NVARCHAR (50) NULL,
    [ColumnName] VARCHAR (50) NULL,
    [OldValue] VARCHAR (1000) NULL,
    [NewValue] VARCHAR (1000) NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_62602a65_3355_47bf_b7c7_957e8151d0e7DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_62602a65_3355_47bf_b7c7_957e8151d0e7',
    FILE_FORMAT = [Informatica_62602a65_3355_47bf_b7c7_957e8151d0e7FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

