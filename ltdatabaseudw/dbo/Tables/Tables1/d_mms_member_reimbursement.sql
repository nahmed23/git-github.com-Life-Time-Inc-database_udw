﻿CREATE TABLE [dbo].[d_mms_member_reimbursement] (
    [d_mms_member_reimbursement_id]                   BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                         CHAR (32)    NOT NULL,
    [fact_mms_member_reimbursement_key]               VARCHAR (32) NULL,
    [member_reimbursement_id]                         INT          NULL,
    [dim_mms_member_key]                              VARCHAR (32) NULL,
    [dim_mms_reimbursement_program_key]               VARCHAR (32) NULL,
    [enrollment_date]                                 DATETIME     NULL,
    [enrollment_dim_date_key]                         VARCHAR (8)  NULL,
    [member_id]                                       INT          NULL,
    [reimbursement_program_id]                        INT          NULL,
    [reimbursement_program_identifier_format_bk_hash] VARCHAR (32) NULL,
    [reimbursement_program_identifier_format_id]      INT          NULL,
    [termination_date]                                DATETIME     NULL,
    [termination_dim_date_key]                        VARCHAR (8)  NULL,
    [p_mms_member_reimbursement_id]                   BIGINT       NOT NULL,
    [deleted_flag]                                    INT          NULL,
    [dv_load_date_time]                               DATETIME     NULL,
    [dv_load_end_date_time]                           DATETIME     NULL,
    [dv_batch_id]                                     BIGINT       NOT NULL,
    [dv_inserted_date_time]                           DATETIME     NOT NULL,
    [dv_insert_user]                                  VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                            DATETIME     NULL,
    [dv_update_user]                                  VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_member_reimbursement]([dv_batch_id] ASC);

