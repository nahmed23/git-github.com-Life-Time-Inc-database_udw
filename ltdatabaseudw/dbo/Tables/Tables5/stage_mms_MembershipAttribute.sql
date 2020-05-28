CREATE TABLE [dbo].[stage_mms_MembershipAttribute] (
    [stage_mms_MembershipAttribute_id] BIGINT       NOT NULL,
    [MembershipAttributeID]            INT          NULL,
    [MembershipID]                     INT          NULL,
    [AttributeValue]                   VARCHAR (50) NULL,
    [ValMembershipAttributeTypeID]     INT          NULL,
    [InsertedDateTime]                 DATETIME     NULL,
    [UpdatedDateTime]                  DATETIME     NULL,
    [EffectiveFromDateTime]            DATETIME     NULL,
    [EffectiveThruDateTime]            DATETIME     NULL,
    [dv_batch_id]                      BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

