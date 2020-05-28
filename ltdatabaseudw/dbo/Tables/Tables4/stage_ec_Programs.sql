CREATE TABLE [dbo].[stage_ec_Programs] (
    [stage_ec_Programs_id] BIGINT          NOT NULL,
    [ProgramId]            INT             NULL,
    [PartyId]              INT             NULL,
    [Name]                 NVARCHAR (4000) NULL,
    [StartDate]            DATETIME        NULL,
    [EndDate]              DATETIME        NULL,
    [CoachPartyId]         INT             NULL,
    [Status]               INT             NULL,
    [SourceId]             NVARCHAR (50)   NULL,
    [SourceType]           INT             NULL,
    [CreatedDate]          DATETIME        NULL,
    [UpdatedDate]          DATETIME        NULL,
    [dv_batch_id]          BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

