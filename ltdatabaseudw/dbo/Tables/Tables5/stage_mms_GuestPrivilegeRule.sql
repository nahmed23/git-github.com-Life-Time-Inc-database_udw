CREATE TABLE [dbo].[stage_mms_GuestPrivilegeRule] (
    [stage_mms_GuestPrivilegeRule_id] BIGINT   NOT NULL,
    [GuestPrivilegeRuleID]            INT      NULL,
    [NumberOfGuests]                  INT      NULL,
    [ValPeriodTypeID]                 INT      NULL,
    [LowClubAccessLevel]              INT      NULL,
    [HighClubAccessLevel]             INT      NULL,
    [MembershipStartDate]             DATETIME NULL,
    [MembershipEndDate]               DATETIME NULL,
    [InsertedDateTime]                DATETIME NULL,
    [UpdatedDateTime]                 DATETIME NULL,
    [ValCardLevelID]                  INT      NULL,
    [dv_batch_id]                     BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

