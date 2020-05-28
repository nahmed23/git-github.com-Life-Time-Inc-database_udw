CREATE TABLE [dbo].[stage_hash_ltfeb_PartyRelationship] (
    [stage_hash_ltfeb_PartyRelationship_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)     NOT NULL,
    [party_relationship_id]                 INT           NULL,
    [from_party_role_id]                    INT           NULL,
    [to_party_role_id]                      INT           NULL,
    [party_relationship_type_id]            INT           NULL,
    [from_date_in_effect]                   DATETIME      NULL,
    [party_relationship_thru_date]          DATETIME      NULL,
    [update_datetime]                       DATETIME      NULL,
    [update_userid]                         NVARCHAR (31) NULL,
    [dv_load_date_time]                     DATETIME      NOT NULL,
    [dv_inserted_date_time]                 DATETIME      NOT NULL,
    [dv_insert_user]                        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                  DATETIME      NULL,
    [dv_update_user]                        VARCHAR (50)  NULL,
    [dv_batch_id]                           BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

