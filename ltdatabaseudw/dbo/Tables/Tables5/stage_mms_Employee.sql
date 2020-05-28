CREATE TABLE [dbo].[stage_mms_Employee] (
    [stage_mms_Employee_id] BIGINT       NOT NULL,
    [EmployeeID]            INT          NULL,
    [ClubID]                INT          NULL,
    [ActiveStatusFlag]      BIT          NULL,
    [FirstName]             VARCHAR (50) NULL,
    [LastName]              VARCHAR (50) NULL,
    [MiddleInt]             VARCHAR (3)  NULL,
    [InsertedDateTime]      DATETIME     NULL,
    [MemberID]              INT          NULL,
    [UpdatedDateTime]       DATETIME     NULL,
    [HireDate]              DATETIME     NULL,
    [TerminationDate]       DATETIME     NULL,
    [dv_batch_id]           BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

