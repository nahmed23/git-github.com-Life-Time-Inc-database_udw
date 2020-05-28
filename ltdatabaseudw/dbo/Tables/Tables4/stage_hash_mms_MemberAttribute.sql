CREATE TABLE [dbo].[stage_hash_mms_MemberAttribute] (
    [stage_hash_mms_MemberAttribute_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)    NOT NULL,
    [MemberAttributeID]                 INT          NULL,
    [MemberID]                          INT          NULL,
    [AttributeValue]                    VARCHAR (50) NULL,
    [ValMemberAttributeTypeID]          SMALLINT     NULL,
    [ExpirationDate]                    DATETIME     NULL,
    [InsertedDateTime]                  DATETIME     NULL,
    [UpdatedDateTime]                   DATETIME     NULL,
    [EffectiveFromDateTime]             DATETIME     NULL,
    [EffectiveThruDateTime]             DATETIME     NULL,
    [dv_load_date_time]                 DATETIME     NOT NULL,
    [dv_batch_id]                       BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

