CREATE TABLE [dbo].[stage_mms_GuestCount] (
    [stage_mms_GuestCount_id] BIGINT   NOT NULL,
    [GuestCountID]            INT      NULL,
    [ClubID]                  INT      NULL,
    [GuestCountDate]          DATETIME NULL,
    [MemberCount]             INT      NULL,
    [NonMemberCount]          INT      NULL,
    [MemberChildCount]        INT      NULL,
    [NonMemberChildCount]     INT      NULL,
    [InsertedDateTime]        DATETIME NULL,
    [UpdatedDateTime]         DATETIME NULL,
    [dv_batch_id]             BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

