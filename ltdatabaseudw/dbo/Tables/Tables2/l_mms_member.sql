CREATE TABLE [dbo].[l_mms_member] (
    [l_mms_member_id]          BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)    NOT NULL,
    [member_id]                INT          NULL,
    [membership_id]            INT          NULL,
    [employer_id]              INT          NULL,
    [val_member_type_id]       INT          NULL,
    [val_name_prefix_id]       INT          NULL,
    [val_name_suffix_id]       INT          NULL,
    [credit_card_account_id]   INT          NULL,
    [siebel_row_id]            VARCHAR (15) NULL,
    [salesforce_prospect_id]   VARCHAR (18) NULL,
    [party_id]                 INT          NULL,
    [last_updated_employee_id] INT          NULL,
    [salesforce_contact_id]    VARCHAR (18) NULL,
    [crm_contact_id]           VARCHAR (36) NULL,
    [dv_load_date_time]        DATETIME     NOT NULL,
    [dv_r_load_source_id]      BIGINT       NOT NULL,
    [dv_inserted_date_time]    DATETIME     NOT NULL,
    [dv_insert_user]           VARCHAR (50) NOT NULL,
    [dv_updated_date_time]     DATETIME     NULL,
    [dv_update_user]           VARCHAR (50) NULL,
    [dv_hash]                  CHAR (32)    NOT NULL,
    [dv_batch_id]              BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_member]
    ON [dbo].[l_mms_member]([bk_hash] ASC, [l_mms_member_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_member]([dv_batch_id] ASC);

