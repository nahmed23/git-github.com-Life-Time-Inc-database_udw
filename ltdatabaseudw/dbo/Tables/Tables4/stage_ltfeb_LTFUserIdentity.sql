CREATE TABLE [dbo].[stage_ltfeb_LTFUserIdentity] (
    [stage_ltfeb_LTFUserIdentity_id]    BIGINT        NOT NULL,
    [party_id]                          INT           NULL,
    [ltf_user_name]                     NVARCHAR (31) NULL,
    [ltf_user_secret_question]          NVARCHAR (51) NULL,
    [ltf_user_secret_answer]            BINARY (20)   NULL,
    [lui_identity_status]               NVARCHAR (39) NULL,
    [lui_identity_status_from_datetime] SMALLDATETIME NULL,
    [lui_identity_status_thru_datetime] SMALLDATETIME NULL,
    [lui_n_failed_attempts]             INT           NULL,
    [lui_user_agreement_version_number] INT           NULL,
    [update_datetime]                   SMALLDATETIME NULL,
    [update_userid]                     NVARCHAR (31) NULL,
    [password_update_datetime]          SMALLDATETIME NULL,
    [password_change_required]          BIT           NULL,
    [last_successful_login_datetime]    DATETIME      NULL,
    [ltf_user_token]                    NVARCHAR (51) NULL,
    [token_expiration_datetime]         DATETIME      NULL,
    [token_create_datetime]             DATETIME      NULL,
    [dv_batch_id]                       BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

