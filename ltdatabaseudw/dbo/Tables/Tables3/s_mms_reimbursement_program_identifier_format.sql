CREATE TABLE [dbo].[s_mms_reimbursement_program_identifier_format] (
    [s_mms_reimbursement_program_identifier_format_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)     NOT NULL,
    [reimbursement_program_identifier_format_id]       INT           NULL,
    [reimbursement_program_id]                         INT           NULL,
    [description]                                      VARCHAR (50)  NULL,
    [active_flag]                                      BIT           NULL,
    [inserted_date_time]                               DATETIME      NULL,
    [updated_date_time]                                DATETIME      NULL,
    [image_url]                                        VARCHAR (250) NULL,
    [image_description]                                VARCHAR (250) NULL,
    [sort_order]                                       INT           NULL,
    [dv_load_date_time]                                DATETIME      NOT NULL,
    [dv_batch_id]                                      BIGINT        NOT NULL,
    [dv_r_load_source_id]                              BIGINT        NOT NULL,
    [dv_inserted_date_time]                            DATETIME      NOT NULL,
    [dv_insert_user]                                   VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                             DATETIME      NULL,
    [dv_update_user]                                   VARCHAR (50)  NULL,
    [dv_hash]                                          CHAR (32)     NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_reimbursement_program_identifier_format]
    ON [dbo].[s_mms_reimbursement_program_identifier_format]([bk_hash] ASC, [s_mms_reimbursement_program_identifier_format_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_reimbursement_program_identifier_format]([dv_batch_id] ASC);

