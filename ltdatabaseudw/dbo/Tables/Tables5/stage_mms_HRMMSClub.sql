CREATE TABLE [dbo].[stage_mms_HRMMSClub] (
    [stage_mms_HRMMSClub_id] BIGINT        NOT NULL,
    [HRMMSClubID]            INT           NULL,
    [HRClub]                 VARCHAR (30)  NULL,
    [MMSClubID]              INT           NULL,
    [ReportingRegionID]      INT           NULL,
    [NetworkClubGMPath]      VARCHAR (100) NULL,
    [InsertedDateTime]       DATETIME      NULL,
    [UpdatedDateTime]        DATETIME      NULL,
    [dv_batch_id]            BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

