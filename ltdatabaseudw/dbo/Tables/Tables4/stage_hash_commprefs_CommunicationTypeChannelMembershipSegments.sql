CREATE TABLE [dbo].[stage_hash_commprefs_CommunicationTypeChannelMembershipSegments] (
    [stage_hash_commprefs_CommunicationTypeChannelMembershipSegments_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                                            CHAR (32)    NOT NULL,
    [Id]                                                                 INT          NULL,
    [Show]                                                               BIT          NULL,
    [OptInDefault]                                                       BIT          NULL,
    [CreatedTime]                                                        DATETIME     NULL,
    [UpdatedTime]                                                        DATETIME     NULL,
    [CommunicationTypeChannelId]                                         INT          NULL,
    [MembershipSegmentId]                                                INT          NULL,
    [dv_load_date_time]                                                  DATETIME     NOT NULL,
    [dv_inserted_date_time]                                              DATETIME     NOT NULL,
    [dv_insert_user]                                                     VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                                               DATETIME     NULL,
    [dv_update_user]                                                     VARCHAR (50) NULL,
    [dv_batch_id]                                                        BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

