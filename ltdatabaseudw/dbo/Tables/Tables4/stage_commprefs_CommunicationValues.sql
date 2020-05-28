CREATE TABLE [dbo].[stage_commprefs_CommunicationValues] (
    [stage_commprefs_CommunicationValues_id] BIGINT         NOT NULL,
    [Id]                                     INT            NULL,
    [Value]                                  NVARCHAR (255) NULL,
    [Token]                                  VARCHAR (8000) NULL,
    [TokenCreatedTime]                       DATETIME       NULL,
    [CreatedTime]                            DATETIME       NULL,
    [UpdatedTime]                            DATETIME       NULL,
    [ChannelKey]                             NVARCHAR (128) NULL,
    [dv_batch_id]                            BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

