CREATE TABLE [dbo].[stage_commprefs_MembershipSegments] (
    [stage_commprefs_MembershipSegments_id] BIGINT        NOT NULL,
    [Id]                                    INT           NULL,
    [Key]                                   NVARCHAR (50) NULL,
    [CreatedTime]                           DATETIME      NULL,
    [UpdatedTime]                           DATETIME      NULL,
    [dv_batch_id]                           BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

