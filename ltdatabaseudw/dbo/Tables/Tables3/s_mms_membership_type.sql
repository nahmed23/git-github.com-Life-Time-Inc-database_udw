CREATE TABLE [dbo].[s_mms_membership_type] (
    [s_mms_membership_type_id]      BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)     NOT NULL,
    [membership_type_id]            INT           NULL,
    [assess_due_flag]               BIT           NULL,
    [inserted_date_time]            DATETIME      NULL,
    [short_term_membership_flag]    BIT           NULL,
    [max_unit_type]                 INT           NULL,
    [express_membership_flag]       BIT           NULL,
    [updated_date_time]             DATETIME      NULL,
    [display_name]                  VARCHAR (50)  NULL,
    [assess_jr_member_dues_flag]    BIT           NULL,
    [waive_admin_fee_flag]          BIT           NULL,
    [gta_sig_override]              VARCHAR (100) NULL,
    [allow_partner_program_flag]    BIT           NULL,
    [min_unit_type]                 INT           NULL,
    [min_primary_age]               INT           NULL,
    [waive_late_fee_flag]           BIT           NULL,
    [suppress_membership_card_flag] BIT           NULL,
    [waive_enrollment_fee_flag]     BIT           NULL,
    [dv_load_date_time]             DATETIME      NOT NULL,
    [dv_batch_id]                   BIGINT        NOT NULL,
    [dv_r_load_source_id]           BIGINT        NOT NULL,
    [dv_inserted_date_time]         DATETIME      NOT NULL,
    [dv_insert_user]                VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]          DATETIME      NULL,
    [dv_update_user]                VARCHAR (50)  NULL,
    [dv_hash]                       CHAR (32)     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_membership_type]
    ON [dbo].[s_mms_membership_type]([bk_hash] ASC, [s_mms_membership_type_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_membership_type]([dv_batch_id] ASC);

