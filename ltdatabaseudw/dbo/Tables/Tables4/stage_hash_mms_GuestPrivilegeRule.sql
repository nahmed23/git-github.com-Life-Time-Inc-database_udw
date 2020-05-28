CREATE TABLE [dbo].[stage_hash_mms_GuestPrivilegeRule] (
    [stage_hash_mms_GuestPrivilegeRule_id] BIGINT    IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32) NOT NULL,
    [GuestPrivilegeRuleID]                 INT       NULL,
    [NumberOfGuests]                       INT       NULL,
    [ValPeriodTypeID]                      INT       NULL,
    [LowClubAccessLevel]                   INT       NULL,
    [HighClubAccessLevel]                  INT       NULL,
    [MembershipStartDate]                  DATETIME  NULL,
    [MembershipEndDate]                    DATETIME  NULL,
    [InsertedDateTime]                     DATETIME  NULL,
    [UpdatedDateTime]                      DATETIME  NULL,
    [ValCardLevelID]                       INT       NULL,
    [dv_load_date_time]                    DATETIME  NOT NULL,
    [dv_batch_id]                          BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

