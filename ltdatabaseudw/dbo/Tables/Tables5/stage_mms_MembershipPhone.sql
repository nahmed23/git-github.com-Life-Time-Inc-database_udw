CREATE TABLE [dbo].[stage_mms_MembershipPhone] (
    [stage_mms_MembershipPhone_id] BIGINT      NOT NULL,
    [MembershipPhoneID]            INT         NULL,
    [MembershipID]                 INT         NULL,
    [AreaCode]                     VARCHAR (3) NULL,
    [ValPhoneTypeID]               TINYINT     NULL,
    [Number]                       VARCHAR (7) NULL,
    [InsertedDateTime]             DATETIME    NULL,
    [UpdatedDateTime]              DATETIME    NULL,
    [dv_batch_id]                  BIGINT      NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

