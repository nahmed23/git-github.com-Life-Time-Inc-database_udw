CREATE TABLE [dbo].[stage_mms_PackageSession] (
    [stage_mms_PackageSession_id] BIGINT         NOT NULL,
    [PackageSessionID]            INT            NULL,
    [PackageID]                   INT            NULL,
    [CreatedDateTime]             DATETIME       NULL,
    [UTCCreatedDateTime]          DATETIME       NULL,
    [CreatedDateTimeZone]         VARCHAR (4)    NULL,
    [ModifiedDateTime]            DATETIME       NULL,
    [UTCModifiedDateTime]         DATETIME       NULL,
    [ModifiedDateTimeZone]        VARCHAR (4)    NULL,
    [DeliveredDateTime]           DATETIME       NULL,
    [UTCDeliveredDateTime]        DATETIME       NULL,
    [DeliveredDateTimeZone]       VARCHAR (4)    NULL,
    [CreatedEmployeeID]           INT            NULL,
    [ModifiedEmployeeID]          INT            NULL,
    [DeliveredEmployeeID]         INT            NULL,
    [ClubID]                      INT            NULL,
    [SessionPrice]                NUMERIC (9, 4) NULL,
    [Comment]                     VARCHAR (255)  NULL,
    [InsertedDateTime]            DATETIME       NULL,
    [UpdatedDateTime]             DATETIME       NULL,
    [MMSTranID]                   INT            NULL,
    [dv_batch_id]                 BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

