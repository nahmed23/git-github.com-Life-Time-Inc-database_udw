CREATE TABLE [dbo].[stage_mms_MembershipTypeAttribute] (
    [stage_mms_MembershipTypeAttribute_id] BIGINT   NOT NULL,
    [MembershipTypeAttributeID]            INT      NULL,
    [MembershipTypeID]                     INT      NULL,
    [ValMembershipTypeAttributeID]         SMALLINT NULL,
    [InsertedDateTime]                     DATETIME NULL,
    [UpdatedDateTime]                      DATETIME NULL,
    [dv_batch_id]                          BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

