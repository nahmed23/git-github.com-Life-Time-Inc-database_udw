CREATE TABLE [dbo].[d_ltfeb_party_role] (
    [d_ltfeb_party_role_id]    BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)     NOT NULL,
    [party_role_id]            INT           NULL,
    [headquarters_facility_id] INT           NULL,
    [party_role_type]          NVARCHAR (39) NULL,
    [pr_party_id]              INT           NULL,
    [p_ltfeb_party_role_id]    BIGINT        NOT NULL,
    [deleted_flag]             INT           NULL,
    [dv_load_date_time]        DATETIME      NULL,
    [dv_load_end_date_time]    DATETIME      NULL,
    [dv_batch_id]              BIGINT        NOT NULL,
    [dv_inserted_date_time]    DATETIME      NOT NULL,
    [dv_insert_user]           VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]     DATETIME      NULL,
    [dv_update_user]           VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_ltfeb_party_role]([dv_batch_id] ASC);

