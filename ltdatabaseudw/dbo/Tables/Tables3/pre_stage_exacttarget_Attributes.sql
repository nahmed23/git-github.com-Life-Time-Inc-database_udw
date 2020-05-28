CREATE TABLE [dbo].[pre_stage_exacttarget_Attributes] (
    [ClientID]      BIGINT         NULL,
    [SubscriberKey] VARCHAR (4000) NULL,
    [EmailAddress]  VARCHAR (4000) NULL,
    [SubscriberID]  BIGINT         NULL,
    [executionid]   BIGINT         NULL
)
WITH (HEAP, DISTRIBUTION = HASH([SubscriberID]));

