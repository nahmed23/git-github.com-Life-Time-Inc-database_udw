CREATE TABLE [dbo].[stage_mms_MembershipCommunicationPreference] (
    [stage_mms_MembershipCommunicationPreference_id] BIGINT   NOT NULL,
    [MembershipCommunicationPreferenceID]            INT      NULL,
    [MembershipID]                                   INT      NULL,
    [ValCommunicationPreferenceID]                   TINYINT  NULL,
    [ActiveFlag]                                     BIT      NULL,
    [InsertedDateTime]                               DATETIME NULL,
    [UpdatedDateTime]                                DATETIME NULL,
    [dv_batch_id]                                    BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

