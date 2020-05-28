CREATE TABLE [dbo].[stage_ec_Plans] (
    [stage_ec_Plans_id] BIGINT          NOT NULL,
    [PlanId]            INT             NULL,
    [ProgramId]         INT             NULL,
    [PartyId]           INT             NULL,
    [Name]              NVARCHAR (4000) NULL,
    [Duration]          NVARCHAR (50)   NULL,
    [DurationType]      INT             NULL,
    [StartDate]         DATETIME        NULL,
    [EndDate]           DATETIME        NULL,
    [CoachPartyId]      INT             NULL,
    [SourceId]          NVARCHAR (50)   NULL,
    [SourceType]        INT             NULL,
    [CreatedDate]       DATETIME        NULL,
    [UpdatedDate]       DATETIME        NULL,
    [dv_batch_id]       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

