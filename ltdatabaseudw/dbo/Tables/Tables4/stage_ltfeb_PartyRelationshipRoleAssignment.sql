CREATE TABLE [dbo].[stage_ltfeb_PartyRelationshipRoleAssignment] (
    [stage_ltfeb_PartyRelationshipRoleAssignment_id] BIGINT        NOT NULL,
    [party_relationship_id]                          INT           NULL,
    [party_relationship_role_type]                   NVARCHAR (39) NULL,
    [assigned_id]                                    VARCHAR (25)  NULL,
    [update_datetime]                                DATETIME      NULL,
    [update_userid]                                  NVARCHAR (31) NULL,
    [dv_batch_id]                                    BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

