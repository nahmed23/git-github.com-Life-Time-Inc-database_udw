CREATE TABLE [dbo].[stage_mms_EmailAddressStatus] (
    [stage_mms_EmailAddressStatus_id]    BIGINT        NOT NULL,
    [EmailAddressStatusID]               INT           NULL,
    [EmailAddress]                       VARCHAR (140) NULL,
    [StatusFromDate]                     DATETIME      NULL,
    [StatusThruDate]                     DATETIME      NULL,
    [InsertedDateTime]                   DATETIME      NULL,
    [UpdatedDateTime]                    DATETIME      NULL,
    [ValCommunicationPreferenceSourceID] TINYINT       NULL,
    [ValCommunicationPreferenceStatusID] TINYINT       NULL,
    [EmailAddressSearch]                 VARCHAR (10)  NULL,
    [dv_batch_id]                        BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

