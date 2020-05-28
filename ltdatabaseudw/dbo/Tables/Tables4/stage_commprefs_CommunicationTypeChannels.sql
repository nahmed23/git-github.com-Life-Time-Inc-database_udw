CREATE TABLE [dbo].[stage_commprefs_CommunicationTypeChannels] (
    [stage_commprefs_CommunicationTypeChannels_id] BIGINT         NOT NULL,
    [Id]                                           INT            NULL,
    [DisplayNameOverride]                          VARCHAR (8000) NULL,
    [CreatedTime]                                  DATETIME       NULL,
    [UpdatedTime]                                  DATETIME       NULL,
    [DeletedTime]                                  DATETIME       NULL,
    [ChannelKey]                                   NVARCHAR (128) NULL,
    [CommunicationTypeId]                          INT            NULL,
    [dv_batch_id]                                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

