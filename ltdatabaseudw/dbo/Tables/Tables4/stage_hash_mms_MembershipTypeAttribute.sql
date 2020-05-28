CREATE TABLE [dbo].[stage_hash_mms_MembershipTypeAttribute] (
    [stage_hash_mms_MembershipTypeAttribute_id] BIGINT    IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32) NOT NULL,
    [MembershipTypeAttributeID]                 INT       NULL,
    [MembershipTypeID]                          INT       NULL,
    [ValMembershipTypeAttributeID]              SMALLINT  NULL,
    [InsertedDateTime]                          DATETIME  NULL,
    [UpdatedDateTime]                           DATETIME  NULL,
    [dv_load_date_time]                         DATETIME  NOT NULL,
    [dv_batch_id]                               BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

