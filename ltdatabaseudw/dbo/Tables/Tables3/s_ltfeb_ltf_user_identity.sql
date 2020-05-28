CREATE TABLE [dbo].[s_ltfeb_ltf_user_identity] (
    [s_ltfeb_ltf_user_identity_id]       BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)     NOT NULL,
    [party_id]                           INT           NULL,
    [ltf_user_name]                      NVARCHAR (31) NULL,
    [ltf_user_secret_question]           NVARCHAR (51) NULL,
    [ltf_user_secret_answer]             BINARY (20)   NULL,
    [lui_identity_status]                NVARCHAR (39) NULL,
    [lui_identity_status_from_date_time] SMALLDATETIME NULL,
    [lui_identity_status_thru_date_time] SMALLDATETIME NULL,
    [lui_n_failed_attempts]              INT           NULL,
    [lui_user_agreement_version_number]  INT           NULL,
    [update_date_time]                   SMALLDATETIME NULL,
    [update_user_id]                     NVARCHAR (31) NULL,
    [password_update_date_time]          SMALLDATETIME NULL,
    [password_change_required]           BIT           NULL,
    [last_successful_login_date_time]    DATETIME      NULL,
    [ltf_user_token]                     NVARCHAR (51) NULL,
    [token_expiration_date_time]         DATETIME      NULL,
    [token_create_date_time]             DATETIME      NULL,
    [dv_load_date_time]                  DATETIME      NOT NULL,
    [dv_r_load_source_id]                BIGINT        NOT NULL,
    [dv_inserted_date_time]              DATETIME      NOT NULL,
    [dv_insert_user]                     VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]               DATETIME      NULL,
    [dv_update_user]                     VARCHAR (50)  NULL,
    [dv_hash]                            CHAR (32)     NOT NULL,
    [dv_batch_id]                        BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ltfeb_ltf_user_identity]([dv_batch_id] ASC);

