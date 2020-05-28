CREATE TABLE [dbo].[stage_hash_pool_pass_CourtesyVisits] (
    [stage_hash_pool_pass_CourtesyVisits_id] BIGINT             IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)          NOT NULL,
    [Id]                                     INT                NULL,
    [ClubId]                                 INT                NULL,
    [EmployeePartyId]                        INT                NULL,
    [MemberPartyId]                          INT                NULL,
    [CreatedDate]                            DATETIMEOFFSET (7) NULL,
    [UpdatedDate]                            DATETIMEOFFSET (7) NULL,
    [dv_load_date_time]                      DATETIME           NOT NULL,
    [dv_batch_id]                            BIGINT             NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

