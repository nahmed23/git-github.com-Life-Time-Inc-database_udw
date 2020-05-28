CREATE EXTERNAL TABLE [dbo].[Informatica_95c23ccb_bc27_4dfd_8144_33f6140c947fExt] (
    [stage_exacttarget_SendLog_id] BIGINT NULL,
    [JobID] BIGINT NULL,
    [ListID] BIGINT NULL,
    [BatchID] BIGINT NULL,
    [SubID] BIGINT NULL,
    [TriggeredSendID] BIGINT NULL,
    [ErrorCode_] VARCHAR (4000) NULL,
    [Member_ID] BIGINT NULL,
    [SubscriberKey] VARCHAR (4000) NULL,
    [EmailAddress] VARCHAR (4000) NULL,
    [InsertedDateTime] DATETIME NULL,
    [jan_one] DATETIME NULL,
    [dv_inserted_date_time] DATETIME NULL,
    [dv_insert_user] VARCHAR (50) NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_95c23ccb_bc27_4dfd_8144_33f6140c947fDS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_95c23ccb_bc27_4dfd_8144_33f6140c947f',
    FILE_FORMAT = [Informatica_95c23ccb_bc27_4dfd_8144_33f6140c947fFF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

