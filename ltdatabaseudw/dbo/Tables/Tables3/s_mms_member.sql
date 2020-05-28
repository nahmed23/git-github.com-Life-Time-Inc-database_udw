CREATE TABLE [dbo].[s_mms_member] (
    [s_mms_member_id]            BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)     NOT NULL,
    [member_id]                  INT           NULL,
    [first_name]                 VARCHAR (50)  NULL,
    [middle_name]                VARCHAR (25)  NULL,
    [last_name]                  VARCHAR (50)  NULL,
    [dob]                        DATETIME      NULL,
    [gender]                     CHAR (1)      NULL,
    [active_flag]                BIT           NULL,
    [has_message_flag]           BIT           NULL,
    [join_date]                  DATETIME      NULL,
    [comment]                    VARCHAR (250) NULL,
    [inserted_date_time]         DATETIME      NULL,
    [email_address]              VARCHAR (140) NULL,
    [charge_to_account_flag]     BIT           NULL,
    [cw_medica_number]           VARCHAR (16)  NULL,
    [cw_enrollment_date]         DATETIME      NULL,
    [cw_program_enrolled_flag]   BIT           NULL,
    [mip_updated_date_time]      DATETIME      NULL,
    [updated_date_time]          DATETIME      NULL,
    [photo_delete_date_time]     DATETIME      NULL,
    [member_token]               BINARY (20)   NULL,
    [assess_jr_member_dues_flag] BIT           NULL,
    [dv_load_date_time]          DATETIME      NOT NULL,
    [dv_r_load_source_id]        BIGINT        NOT NULL,
    [dv_inserted_date_time]      DATETIME      NOT NULL,
    [dv_insert_user]             VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]       DATETIME      NULL,
    [dv_update_user]             VARCHAR (50)  NULL,
    [dv_hash]                    CHAR (32)     NOT NULL,
    [dv_batch_id]                BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_member]
    ON [dbo].[s_mms_member]([bk_hash] ASC, [s_mms_member_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_member]([dv_batch_id] ASC);

