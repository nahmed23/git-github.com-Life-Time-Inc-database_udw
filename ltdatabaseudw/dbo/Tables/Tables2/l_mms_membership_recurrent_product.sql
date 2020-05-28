CREATE TABLE [dbo].[l_mms_membership_recurrent_product] (
    [l_mms_membership_recurrent_product_id]       BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)    NOT NULL,
    [membership_recurrent_product_id]             INT          NULL,
    [membership_id]                               INT          NULL,
    [product_id]                                  INT          NULL,
    [val_recurrent_product_termination_reason_id] SMALLINT     NULL,
    [club_id]                                     INT          NULL,
    [last_updated_employee_id]                    INT          NULL,
    [commission_employee_id]                      INT          NULL,
    [member_id]                                   INT          NULL,
    [val_recurrent_product_source_id]             TINYINT      NULL,
    [val_assessment_day_id]                       SMALLINT     NULL,
    [pricing_discount_id]                         INT          NULL,
    [val_discount_reason_id]                      SMALLINT     NULL,
    [dv_load_date_time]                           DATETIME     NOT NULL,
    [dv_batch_id]                                 BIGINT       NOT NULL,
    [dv_r_load_source_id]                         BIGINT       NOT NULL,
    [dv_inserted_date_time]                       DATETIME     NOT NULL,
    [dv_insert_user]                              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                        DATETIME     NULL,
    [dv_update_user]                              VARCHAR (50) NULL,
    [dv_hash]                                     CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_mms_membership_recurrent_product]
    ON [dbo].[l_mms_membership_recurrent_product]([bk_hash] ASC, [l_mms_membership_recurrent_product_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_mms_membership_recurrent_product]([dv_batch_id] ASC);

