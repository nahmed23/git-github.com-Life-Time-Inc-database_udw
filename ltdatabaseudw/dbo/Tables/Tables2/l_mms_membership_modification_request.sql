CREATE TABLE [dbo].[l_mms_membership_modification_request] (
    [l_mms_membership_modification_request_id]      BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)    NOT NULL,
    [membership_modification_request_id]            INT          NULL,
    [membership_id]                                 INT          NULL,
    [member_id]                                     INT          NULL,
    [val_membership_modification_request_type_id]   TINYINT      NULL,
    [val_flex_reason_id]                            BIGINT       NULL,
    [membership_type_id]                            INT          NULL,
    [val_membership_modification_request_status_id] BIGINT       NULL,
    [employee_id]                                   INT          NULL,
    [val_membership_upgrade_date_range_id]          BIGINT       NULL,
    [club_id]                                       INT          NULL,
    [commisioned_employee_id]                       INT          NULL,
    [member_agreement_staging_id]                   INT          NULL,
    [previous_membership_type_id]                   INT          NULL,
    [dv_load_date_time]                             DATETIME     NOT NULL,
    [dv_r_load_source_id]                           BIGINT       NOT NULL,
    [dv_inserted_date_time]                         DATETIME     NOT NULL,
    [dv_insert_user]                                VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                          DATETIME     NULL,
    [dv_update_user]                                VARCHAR (50) NULL,
    [dv_hash]                                       CHAR (32)    NOT NULL,
    [dv_batch_id]                                   BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_membership_modification_request]([dv_batch_id] ASC);

