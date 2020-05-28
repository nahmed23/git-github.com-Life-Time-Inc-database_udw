CREATE TABLE [dbo].[p_ltfeb_party_relationship_role_assignment] (
    [p_ltfeb_party_relationship_role_assignment_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)     NOT NULL,
    [party_relationship_id]                         INT           NULL,
    [party_relationship_role_type]                  NVARCHAR (39) NULL,
    [l_ltfeb_party_relationship_role_assignment_id] BIGINT        NULL,
    [s_ltfeb_party_relationship_role_assignment_id] BIGINT        NULL,
    [dv_load_date_time]                             DATETIME      NOT NULL,
    [dv_load_end_date_time]                         DATETIME      NOT NULL,
    [dv_greatest_satellite_date_time]               DATETIME      NULL,
    [dv_next_greatest_satellite_date_time]          DATETIME      NULL,
    [dv_first_in_key_series]                        INT           NULL,
    [dv_inserted_date_time]                         DATETIME      NOT NULL,
    [dv_insert_user]                                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                          DATETIME      NULL,
    [dv_update_user]                                VARCHAR (50)  NULL,
    [dv_batch_id]                                   BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_ltfeb_party_relationship_role_assignment]
    ON [dbo].[p_ltfeb_party_relationship_role_assignment]([bk_hash] ASC, [p_ltfeb_party_relationship_role_assignment_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_ltfeb_party_relationship_role_assignment]([dv_batch_id] ASC);

