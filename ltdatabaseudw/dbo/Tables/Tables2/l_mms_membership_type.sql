CREATE TABLE [dbo].[l_mms_membership_type] (
    [l_mms_membership_type_id]             BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [membership_type_id]                   INT          NULL,
    [product_id]                           INT          NULL,
    [val_membership_type_group_id]         INT          NULL,
    [val_check_in_group_id]                INT          NULL,
    [val_membership_type_family_status_id] INT          NULL,
    [val_enrollment_type_id]               INT          NULL,
    [val_unit_type_id]                     INT          NULL,
    [member_card_design_id]                INT          NULL,
    [val_welcome_kit_type_id]              INT          NULL,
    [val_pricing_method_id]                INT          NULL,
    [val_pricing_rule_id]                  INT          NULL,
    [val_restricted_group_id]              INT          NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_batch_id]                          BIGINT       NOT NULL,
    [dv_r_load_source_id]                  BIGINT       NOT NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_hash]                              CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_membership_type]
    ON [dbo].[l_mms_membership_type]([bk_hash] ASC, [l_mms_membership_type_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_membership_type]([dv_batch_id] ASC);

