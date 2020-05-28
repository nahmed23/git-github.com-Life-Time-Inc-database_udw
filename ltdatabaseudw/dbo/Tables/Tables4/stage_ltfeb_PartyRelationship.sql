CREATE TABLE [dbo].[stage_ltfeb_PartyRelationship] (
    [stage_ltfeb_PartyRelationship_id] BIGINT        NOT NULL,
    [party_relationship_id]            INT           NULL,
    [from_party_role_id]               INT           NULL,
    [to_party_role_id]                 INT           NULL,
    [party_relationship_type_id]       INT           NULL,
    [from_date_in_effect]              DATETIME      NULL,
    [party_relationship_thru_date]     DATETIME      NULL,
    [update_datetime]                  DATETIME      NULL,
    [update_userid]                    NVARCHAR (31) NULL,
    [dv_batch_id]                      BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

