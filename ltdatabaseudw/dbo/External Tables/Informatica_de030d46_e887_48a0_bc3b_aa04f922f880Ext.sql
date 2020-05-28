CREATE EXTERNAL TABLE [dbo].[Informatica_de030d46_e887_48a0_bc3b_aa04f922f880Ext] (
    [stage_exacttarget_Subscribers_id] BIGINT NULL,
    [ClientID] BIGINT NULL,
    [SubscriberKey] VARCHAR (4000) NULL,
    [EmailAddress] VARCHAR (4000) NULL,
    [SubscriberID] BIGINT NULL,
    [Status] VARCHAR (4000) NULL,
    [DateHeld] DATETIME NULL,
    [DateCreated] DATETIME NULL,
    [DateUnsubscribed] DATETIME NULL,
    [jan_one] DATETIME NULL,
    [dv_inserted_date_time] DATETIME NULL,
    [dv_insert_user] VARCHAR (50) NULL,
    [dv_batch_id] BIGINT NULL
)
    WITH (
    DATA_SOURCE = [Informatica_de030d46_e887_48a0_bc3b_aa04f922f880DS],
    LOCATION = N'592f0ecc-9312-4936-b9db-184dcd0a48c8/Informatica_de030d46_e887_48a0_bc3b_aa04f922f880',
    FILE_FORMAT = [Informatica_de030d46_e887_48a0_bc3b_aa04f922f880FF],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );

