CREATE TABLE [dbo].[l_ltfeb_party_relationship_role_assignment] (
    [l_ltfeb_party_relationship_role_assignment_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)     NOT NULL,
    [party_relationship_id]                         INT           NULL,
    [party_relationship_role_type]                  NVARCHAR (39) NULL,
    [assigned_id]                                   VARCHAR (25)  NULL,
    [dv_load_date_time]                             DATETIME      NOT NULL,
    [dv_r_load_source_id]                           BIGINT        NOT NULL,
    [dv_inserted_date_time]                         DATETIME      NOT NULL,
    [dv_insert_user]                                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                          DATETIME      NULL,
    [dv_update_user]                                VARCHAR (50)  NULL,
    [dv_hash]                                       CHAR (32)     NOT NULL,
    [dv_batch_id]                                   BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_ltfeb_party_relationship_role_assignment]
    ON [dbo].[l_ltfeb_party_relationship_role_assignment]([bk_hash] ASC, [l_ltfeb_party_relationship_role_assignment_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ltfeb_party_relationship_role_assignment]([dv_batch_id] ASC);

