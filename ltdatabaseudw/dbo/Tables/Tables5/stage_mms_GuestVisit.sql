CREATE TABLE [dbo].[stage_mms_GuestVisit] (
    [stage_mms_GuestVisit_id] BIGINT       NOT NULL,
    [GuestVisitID]            INT          NULL,
    [GuestID]                 INT          NULL,
    [ClubID]                  INT          NULL,
    [VisitDateTime]           DATETIME     NULL,
    [ValGuestAccessMethodID]  INT          NULL,
    [MemberID]                INT          NULL,
    [InsertedDateTime]        DATETIME     NULL,
    [UpdatedDateTime]         DATETIME     NULL,
    [EmployeeID]              INT          NULL,
    [Comment]                 VARCHAR (50) NULL,
    [PromotionCode]           VARCHAR (50) NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

