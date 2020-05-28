CREATE TABLE [dbo].[stage_commprefs_PartyCommunicationValues] (
    [stage_commprefs_PartyCommunicationValues_id] BIGINT          NOT NULL,
    [Id]                                          INT             NULL,
    [CreatedTime]                                 DATETIME        NULL,
    [CreatedBy]                                   NVARCHAR (4000) NULL,
    [DeletedTime]                                 DATETIME        NULL,
    [DeletedBy]                                   NVARCHAR (4000) NULL,
    [CommunicationValueId]                        INT             NULL,
    [PartyId]                                     INT             NULL,
    [jan_one]                                     DATETIME        NULL,
    [dv_batch_id]                                 BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

