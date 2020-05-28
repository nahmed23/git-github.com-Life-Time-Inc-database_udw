CREATE TABLE [dbo].[s_mms_membership] (
    [s_mms_membership_id]                     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [membership_id]                           INT             NULL,
    [legacy_code]                             VARCHAR (12)    NULL,
    [activation_date]                         DATETIME        NULL,
    [expiration_date]                         DATETIME        NULL,
    [total_contract_amount]                   DECIMAL (26, 6) NULL,
    [comments]                                VARCHAR (255)   NULL,
    [mandatory_comment_flag]                  BIT             NULL,
    [cancellation_request_date]               DATETIME        NULL,
    [created_date_time]                       DATETIME        NULL,
    [utc_created_date_time]                   DATETIME        NULL,
    [created_date_time_zone]                  VARCHAR (4)     NULL,
    [inserted_date_time]                      DATETIME        NULL,
    [updated_date_time]                       DATETIME        NULL,
    [money_back_cancel_policy_days]           INT             NULL,
    [join_fee_paid]                           DECIMAL (26, 6) NULL,
    [child_center_unrestricted_checkout_flag] BIT             NULL,
    [current_price]                           DECIMAL (26, 6) NULL,
    [prior_plus_price]                        DECIMAL (26, 6) NULL,
    [dv_load_date_time]                       DATETIME        NOT NULL,
    [dv_r_load_source_id]                     BIGINT          NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL,
    [dv_hash]                                 CHAR (32)       NOT NULL,
    [dv_batch_id]                             BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_mms_membership]
    ON [dbo].[s_mms_membership]([bk_hash] ASC, [s_mms_membership_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_mms_membership]([dv_batch_id] ASC);

