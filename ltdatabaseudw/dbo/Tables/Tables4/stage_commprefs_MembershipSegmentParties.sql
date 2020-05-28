CREATE TABLE [dbo].[stage_commprefs_MembershipSegmentParties] (
    [stage_commprefs_MembershipSegmentParties_id] BIGINT   NOT NULL,
    [MembershipSegmentId]                         INT      NULL,
    [PartyId]                                     INT      NULL,
    [jan_one]                                     DATETIME NULL,
    [dv_batch_id]                                 BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

