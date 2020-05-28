CREATE TABLE [dbo].[stage_commprefs_CommunicationPreferences] (
    [stage_commprefs_CommunicationPreferences_id] BIGINT         NOT NULL,
    [Id]                                          INT            NULL,
    [OptIn]                                       BIT            NULL,
    [EffectiveTime]                               DATETIME       NULL,
    [CreatedTime]                                 DATETIME       NULL,
    [UpdatedTime]                                 DATETIME       NULL,
    [UpdatedBy]                                   VARCHAR (8000) NULL,
    [CommunicationTypeChannelId]                  INT            NULL,
    [CommunicationValueId]                        INT            NULL,
    [dv_batch_id]                                 BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

