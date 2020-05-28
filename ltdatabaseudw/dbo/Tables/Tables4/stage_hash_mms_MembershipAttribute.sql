CREATE TABLE [dbo].[stage_hash_mms_MembershipAttribute] (
    [stage_hash_mms_MembershipAttribute_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)    NOT NULL,
    [MembershipAttributeID]                 INT          NULL,
    [MembershipID]                          INT          NULL,
    [AttributeValue]                        VARCHAR (50) NULL,
    [ValMembershipAttributeTypeID]          INT          NULL,
    [InsertedDateTime]                      DATETIME     NULL,
    [UpdatedDateTime]                       DATETIME     NULL,
    [EffectiveFromDateTime]                 DATETIME     NULL,
    [EffectiveThruDateTime]                 DATETIME     NULL,
    [dv_load_date_time]                     DATETIME     NOT NULL,
    [dv_updated_date_time]                  DATETIME     NULL,
    [dv_update_user]                        VARCHAR (50) NULL,
    [dv_batch_id]                           BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

