CREATE TABLE [dbo].[d_mms_reimbursement_program] (
    [d_mms_reimbursement_program_id]               BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)      NOT NULL,
    [dim_mms_reimbursement_program_key]            CHAR (32)      NULL,
    [reimbursement_program_id]                     INT            NULL,
    [dim_mms_company_key]                          CHAR (32)      NULL,
    [program_active_flag]                          CHAR (1)       NULL,
    [program_name]                                 VARCHAR (50)   NULL,
    [program_processing_type_dim_description_key]  VARCHAR (2000) NULL,
    [program_type_dim_description_key]             VARCHAR (2000) NULL,
    [val_reimbursement_program_processing_type_id] INT            NULL,
    [val_reimbursement_program_type_id]            INT            NULL,
    [deleted_flag]                                 INT            NULL,
    [p_mms_reimbursement_program_id]               BIGINT         NOT NULL,
    [dv_load_date_time]                            DATETIME       NULL,
    [dv_load_end_date_time]                        DATETIME       NULL,
    [dv_batch_id]                                  BIGINT         NOT NULL,
    [dv_inserted_date_time]                        DATETIME       NOT NULL,
    [dv_insert_user]                               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                         DATETIME       NULL,
    [dv_update_user]                               VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_reimbursement_program]([dv_batch_id] ASC);

