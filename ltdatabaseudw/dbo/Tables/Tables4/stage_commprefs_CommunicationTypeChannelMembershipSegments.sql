CREATE TABLE [dbo].[stage_commprefs_CommunicationTypeChannelMembershipSegments] (
    [stage_commprefs_CommunicationTypeChannelMembershipSegments_id] BIGINT   NOT NULL,
    [Id]                                                            INT      NULL,
    [Show]                                                          BIT      NULL,
    [OptInDefault]                                                  BIT      NULL,
    [CreatedTime]                                                   DATETIME NULL,
    [UpdatedTime]                                                   DATETIME NULL,
    [CommunicationTypeChannelId]                                    INT      NULL,
    [MembershipSegmentId]                                           INT      NULL,
    [dv_batch_id]                                                   BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

