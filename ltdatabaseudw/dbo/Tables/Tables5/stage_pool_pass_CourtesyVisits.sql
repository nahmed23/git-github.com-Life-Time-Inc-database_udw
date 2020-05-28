CREATE TABLE [dbo].[stage_pool_pass_CourtesyVisits] (
    [stage_pool_pass_CourtesyVisits_id] BIGINT             NOT NULL,
    [Id]                                INT                NULL,
    [ClubId]                            INT                NULL,
    [EmployeePartyId]                   INT                NULL,
    [MemberPartyId]                     INT                NULL,
    [CreatedDate]                       DATETIMEOFFSET (7) NULL,
    [UpdatedDate]                       DATETIMEOFFSET (7) NULL,
    [dv_batch_id]                       BIGINT             NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

