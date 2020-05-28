CREATE TABLE [dbo].[stage_mms_ValMembershipMessageType] (
    [stage_mms_ValMembershipMessageType_id] BIGINT       NOT NULL,
    [ValMembershipMessageTypeID]            INT          NULL,
    [Description]                           VARCHAR (50) NULL,
    [SortOrder]                             INT          NULL,
    [AutoCloseFlag]                         BIT          NULL,
    [ValMessageSeverityID]                  INT          NULL,
    [InsertedDateTime]                      DATETIME     NULL,
    [Abbreviation]                          VARCHAR (20) NULL,
    [EFTSingleOpenFlag]                     BIT          NULL,
    [UpdatedDateTime]                       DATETIME     NULL,
    [dv_batch_id]                           BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

