CREATE EXTERNAL TABLE [dbo].[Informatica_7ef161c9_dedf_4500_a47e_f0e9ae7459c5Ext] (
    [stage_exacttarget_Unsubs_id] BIGINT NULL,
    [ClientID] BIGINT NULL,
    [SendID] BIGINT NULL,
    [SubscriberKey] VARCHAR (4000) NULL,
    [EmailAddress] VARCHAR (4000) NULL,
    [SubscriberID] BIGINT NULL,
    [ListID] BIGINT NULL,
    [EventDate] DATETIME NULL,
    [EventType] VARCHAR (4000) NULL,
    [BatchID] VARCHAR (4000) NULL,
    [TriggeredSendExternalKey] VARCHAR (4000) NULL,
    [UnsubReason] VARCHAR (4000) NULL,
    [jan_one] DATETIME NULL,
    [dv_inserted_date_time] DATETIME NULL,
    [dv_insert_user] VARCHAR (50) NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_7ef161c9_dedf_4500_a47e_f0e9ae7459c5DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_7ef161c9_dedf_4500_a47e_f0e9ae7459c5',
    FILE_FORMAT = [Informatica_7ef161c9_dedf_4500_a47e_f0e9ae7459c5FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

