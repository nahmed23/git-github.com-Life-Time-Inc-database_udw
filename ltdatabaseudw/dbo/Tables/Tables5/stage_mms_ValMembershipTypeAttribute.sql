CREATE TABLE [dbo].[stage_mms_ValMembershipTypeAttribute] (
    [stage_mms_ValMembershipTypeAttribute_id] BIGINT        NOT NULL,
    [ValMembershipTypeAttributeID]            INT           NULL,
    [Description]                             VARCHAR (100) NULL,
    [SortOrder]                               INT           NULL,
    [InsertedDateTime]                        DATETIME      NULL,
    [UpdatedDateTime]                         DATETIME      NULL,
    [dv_batch_id]                             BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

