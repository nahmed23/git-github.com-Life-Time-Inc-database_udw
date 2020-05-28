CREATE TABLE [dbo].[stage_ltfeb_PartyRole] (
    [stage_ltfeb_PartyRole_id] BIGINT        NOT NULL,
    [party_role_id]            INT           NULL,
    [pr_party_id]              INT           NULL,
    [party_role_type]          NVARCHAR (39) NULL,
    [headquarters_facility_id] INT           NULL,
    [update_datetime]          SMALLDATETIME NULL,
    [update_userid]            NVARCHAR (31) NULL,
    [dv_batch_id]              BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

