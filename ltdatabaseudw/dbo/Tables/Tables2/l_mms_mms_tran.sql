CREATE TABLE [dbo].[l_mms_mms_tran] (
    [l_mms_mms_tran_id]              BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)    NOT NULL,
    [mms_tran_id]                    INT          NULL,
    [club_id]                        INT          NULL,
    [membership_id]                  INT          NULL,
    [member_id]                      INT          NULL,
    [drawer_activity_id]             INT          NULL,
    [tran_voided_id]                 INT          NULL,
    [reason_code_id]                 INT          NULL,
    [val_tran_type_id]               TINYINT      NULL,
    [employee_id]                    INT          NULL,
    [original_drawer_activity_id]    INT          NULL,
    [original_mms_tran_id]           INT          NULL,
    [tran_edited_employee_id]        INT          NULL,
    [val_currency_code_id]           TINYINT      NULL,
    [corporate_partner_id]           INT          NULL,
    [converted_val_currency_code_id] TINYINT      NULL,
    [reimbursement_program_id]       INT          NULL,
    [dv_load_date_time]              DATETIME     NOT NULL,
    [dv_r_load_source_id]            BIGINT       NOT NULL,
    [dv_inserted_date_time]          DATETIME     NOT NULL,
    [dv_insert_user]                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]           DATETIME     NULL,
    [dv_update_user]                 VARCHAR (50) NULL,
    [dv_hash]                        CHAR (32)    NOT NULL,
    [dv_deleted]                     BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                    BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_mms_tran]([dv_batch_id] ASC);

