CREATE TABLE [dbo].[stage_mms_MemberAttribute] (
    [stage_mms_MemberAttribute_id] BIGINT       NOT NULL,
    [MemberAttributeID]            INT          NULL,
    [MemberID]                     INT          NULL,
    [AttributeValue]               VARCHAR (50) NULL,
    [ValMemberAttributeTypeID]     SMALLINT     NULL,
    [ExpirationDate]               DATETIME     NULL,
    [InsertedDateTime]             DATETIME     NULL,
    [UpdatedDateTime]              DATETIME     NULL,
    [EffectiveFromDateTime]        DATETIME     NULL,
    [EffectiveThruDateTime]        DATETIME     NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

