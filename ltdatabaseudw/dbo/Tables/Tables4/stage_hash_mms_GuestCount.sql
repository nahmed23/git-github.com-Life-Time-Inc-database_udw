CREATE TABLE [dbo].[stage_hash_mms_GuestCount] (
    [stage_hash_mms_GuestCount_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [GuestCountID]                 INT          NULL,
    [ClubID]                       INT          NULL,
    [GuestCountDate]               DATETIME     NULL,
    [MemberCount]                  INT          NULL,
    [NonMemberCount]               INT          NULL,
    [MemberChildCount]             INT          NULL,
    [NonMemberChildCount]          INT          NULL,
    [InsertedDateTime]             DATETIME     NULL,
    [UpdatedDateTime]              DATETIME     NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

