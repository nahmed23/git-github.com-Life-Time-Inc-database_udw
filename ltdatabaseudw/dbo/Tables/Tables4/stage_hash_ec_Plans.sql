CREATE TABLE [dbo].[stage_hash_ec_Plans] (
    [stage_hash_ec_Plans_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)       NOT NULL,
    [PlanId]                 INT             NULL,
    [ProgramId]              INT             NULL,
    [PartyId]                INT             NULL,
    [Name]                   NVARCHAR (4000) NULL,
    [Duration]               NVARCHAR (50)   NULL,
    [DurationType]           INT             NULL,
    [StartDate]              DATETIME        NULL,
    [EndDate]                DATETIME        NULL,
    [CoachPartyId]           INT             NULL,
    [SourceId]               NVARCHAR (50)   NULL,
    [SourceType]             INT             NULL,
    [CreatedDate]            DATETIME        NULL,
    [UpdatedDate]            DATETIME        NULL,
    [dv_load_date_time]      DATETIME        NOT NULL,
    [dv_updated_date_time]   DATETIME        NULL,
    [dv_update_user]         VARCHAR (50)    NULL,
    [dv_batch_id]            BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

