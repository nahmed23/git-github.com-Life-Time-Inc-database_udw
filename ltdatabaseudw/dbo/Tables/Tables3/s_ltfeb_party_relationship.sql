CREATE TABLE [dbo].[s_ltfeb_party_relationship] (
    [s_ltfeb_party_relationship_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)     NOT NULL,
    [party_relationship_id]         INT           NULL,
    [from_date_in_effect]           DATETIME      NULL,
    [party_relationship_thru_date]  DATETIME      NULL,
    [update_date_time]              DATETIME      NULL,
    [update_user_id]                NVARCHAR (31) NULL,
    [dv_load_date_time]             DATETIME      NOT NULL,
    [dv_r_load_source_id]           BIGINT        NOT NULL,
    [dv_inserted_date_time]         DATETIME      NOT NULL,
    [dv_insert_user]                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]          DATETIME      NULL,
    [dv_update_user]                VARCHAR (50)  NULL,
    [dv_hash]                       CHAR (32)     NOT NULL,
    [dv_batch_id]                   BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ltfeb_party_relationship]([dv_batch_id] ASC);

