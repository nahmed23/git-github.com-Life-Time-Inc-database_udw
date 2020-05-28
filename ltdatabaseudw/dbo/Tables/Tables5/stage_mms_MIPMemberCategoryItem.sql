CREATE TABLE [dbo].[stage_mms_MIPMemberCategoryItem] (
    [stage_mms_MIPMemberCategoryItem_id] BIGINT   NOT NULL,
    [MIPMemberCategoryItemID]            INT      NULL,
    [MemberID]                           INT      NULL,
    [MIPCategoryItemID]                  INT      NULL,
    [InsertedDateTime]                   DATETIME NULL,
    [UpdatedDateTime]                    DATETIME NULL,
    [ClubID]                             INT      NULL,
    [EmailFlag]                          BIT      NULL,
    [dv_batch_id]                        BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

