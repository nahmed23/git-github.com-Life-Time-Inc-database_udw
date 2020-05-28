CREATE TABLE [dbo].[stage_mms_MIPMemberdepartmentunit] (
    [stage_mms_MIPMemberdepartmentunit_id] BIGINT   NOT NULL,
    [MIPMemberdepartmentunitID]            INT      NULL,
    [departmentEmailSentFlag]              BIT      NULL,
    [departmentunitID]                     INT      NULL,
    [MemberID]                             INT      NULL,
    [InsertedDateTime]                     DATETIME NULL,
    [UpdatedDateTime]                      DATETIME NULL,
    [dv_batch_id]                          BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

