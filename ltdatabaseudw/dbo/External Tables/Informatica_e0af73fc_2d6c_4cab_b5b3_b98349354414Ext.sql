CREATE EXTERNAL TABLE [dbo].[Informatica_e0af73fc_2d6c_4cab_b5b3_b98349354414Ext] (
    [stage_mms_ValMIPCategory_id] BIGINT NULL,
    [ValMIPCategoryID] SMALLINT NULL,
    [Description] VARCHAR (50) NULL,
    [SortOrder] SMALLINT NULL,
    [InsertedDateTime] DATETIME NULL,
    [UpdatedDateTime] DATETIME NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_e0af73fc_2d6c_4cab_b5b3_b98349354414DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_e0af73fc_2d6c_4cab_b5b3_b98349354414',
    FILE_FORMAT = [Informatica_e0af73fc_2d6c_4cab_b5b3_b98349354414FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

