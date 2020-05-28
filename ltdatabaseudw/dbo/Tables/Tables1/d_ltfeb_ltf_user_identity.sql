CREATE TABLE [dbo].[d_ltfeb_ltf_user_identity] (
    [d_ltfeb_ltf_user_identity_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)     NOT NULL,
    [party_id]                     INT           NULL,
    [ltf_user_name]                NVARCHAR (31) NULL,
    [p_ltfeb_ltf_user_identity_id] BIGINT        NOT NULL,
    [deleted_flag]                 INT           NULL,
    [dv_load_date_time]            DATETIME      NULL,
    [dv_load_end_date_time]        DATETIME      NULL,
    [dv_batch_id]                  BIGINT        NOT NULL,
    [dv_inserted_date_time]        DATETIME      NOT NULL,
    [dv_insert_user]               VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]         DATETIME      NULL,
    [dv_update_user]               VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

