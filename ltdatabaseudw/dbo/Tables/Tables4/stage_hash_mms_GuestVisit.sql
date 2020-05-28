CREATE TABLE [dbo].[stage_hash_mms_GuestVisit] (
    [stage_hash_mms_GuestVisit_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [GuestVisitID]                 INT          NULL,
    [GuestID]                      INT          NULL,
    [ClubID]                       INT          NULL,
    [VisitDateTime]                DATETIME     NULL,
    [ValGuestAccessMethodID]       INT          NULL,
    [MemberID]                     INT          NULL,
    [InsertedDateTime]             DATETIME     NULL,
    [UpdatedDateTime]              DATETIME     NULL,
    [EmployeeID]                   INT          NULL,
    [Comment]                      VARCHAR (50) NULL,
    [PromotionCode]                VARCHAR (50) NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

