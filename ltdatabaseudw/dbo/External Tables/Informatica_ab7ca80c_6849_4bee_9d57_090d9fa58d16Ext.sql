CREATE EXTERNAL TABLE [dbo].[Informatica_ab7ca80c_6849_4bee_9d57_090d9fa58d16Ext] (
    [stage_exacttarget_Clicks_id] BIGINT NULL,
    [ClientID] BIGINT NULL,
    [SendID] BIGINT NULL,
    [SubscriberKey] VARCHAR (4000) NULL,
    [EmailAddress] VARCHAR (4000) NULL,
    [SubscriberID] BIGINT NULL,
    [ListID] BIGINT NULL,
    [EventDate] DATETIME NULL,
    [EventType] VARCHAR (4000) NULL,
    [SendURLID] BIGINT NULL,
    [URLID] BIGINT NULL,
    [URL] VARCHAR (4000) NULL,
    [Alias] VARCHAR (4000) NULL,
    [BatchID] VARCHAR (4000) NULL,
    [TriggeredSendExternalKey] VARCHAR (4000) NULL,
    [jan_one] DATETIME NULL,
    [dv_inserted_date_time] DATETIME NULL,
    [dv_insert_user] VARCHAR (50) NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_ab7ca80c_6849_4bee_9d57_090d9fa58d16DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_ab7ca80c_6849_4bee_9d57_090d9fa58d16',
    FILE_FORMAT = [Informatica_ab7ca80c_6849_4bee_9d57_090d9fa58d16FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

