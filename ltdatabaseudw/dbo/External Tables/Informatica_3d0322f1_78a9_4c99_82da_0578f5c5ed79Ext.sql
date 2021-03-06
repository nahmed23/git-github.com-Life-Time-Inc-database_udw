﻿CREATE EXTERNAL TABLE [dbo].[Informatica_3d0322f1_78a9_4c99_82da_0578f5c5ed79Ext] (
    [stage_exacttarget_ClickImpression_id] BIGINT NULL,
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
    [ImpressionRegionName] VARCHAR (4000) NULL,
    [jan_one] DATETIME NULL,
    [dv_inserted_date_time] DATETIME NULL,
    [dv_insert_user] VARCHAR (50) NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_3d0322f1_78a9_4c99_82da_0578f5c5ed79DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_3d0322f1_78a9_4c99_82da_0578f5c5ed79',
    FILE_FORMAT = [Informatica_3d0322f1_78a9_4c99_82da_0578f5c5ed79FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

