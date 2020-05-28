CREATE TABLE [dbo].[pre_stage_exacttarget_subscribers_2] (
    [ClientID]         INT            NULL,
    [SubscriberKey]    VARCHAR (4000) NULL,
    [EmailAddress]     VARCHAR (4000) NULL,
    [Status]           VARCHAR (4000) NULL,
    [SubscriberID]     INT            NULL,
    [DateHeld]         DATETIME       NULL,
    [DateCreated]      DATETIME       NULL,
    [DateUnsubscribed] DATETIME       NULL,
    [executionid]      BIGINT         NULL
)
WITH (HEAP, DISTRIBUTION = HASH([SubscriberID]));

