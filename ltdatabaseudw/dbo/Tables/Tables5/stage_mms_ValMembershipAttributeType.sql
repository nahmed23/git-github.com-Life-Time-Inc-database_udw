CREATE TABLE [dbo].[stage_mms_ValMembershipAttributeType] (
    [stage_mms_ValMembershipAttributeType_id] BIGINT       NOT NULL,
    [ValMembershipAttributeTypeID]            INT          NULL,
    [Description]                             VARCHAR (50) NULL,
    [SortOrder]                               INT          NULL,
    [InsertedDateTime]                        DATETIME     NULL,
    [UpdatedDateTime]                         DATETIME     NULL,
    [DisplayUIFlag]                           BIT          NULL,
    [dv_batch_id]                             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

