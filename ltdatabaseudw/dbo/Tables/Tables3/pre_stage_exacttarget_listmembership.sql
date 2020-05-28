CREATE TABLE [dbo].[pre_stage_exacttarget_listmembership] (
    [clientid]          BIGINT        NULL,
    [subscriberKey]     VARCHAR (100) NULL,
    [emailaddress]      VARCHAR (100) NULL,
    [subscriberid]      BIGINT        NULL,
    [listid]            BIGINT        NULL,
    [listname]          VARCHAR (100) NULL,
    [datejoined]        DATETIME      NULL,
    [jointype]          VARCHAR (20)  NULL,
    [dateunsubscribed]  DATETIME      NULL,
    [unsubscribereason] VARCHAR (100) NULL,
    [executionid]       BIGINT        NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

