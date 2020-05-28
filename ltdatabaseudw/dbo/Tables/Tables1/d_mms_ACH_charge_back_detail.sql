CREATE TABLE [dbo].[d_mms_ACH_charge_back_detail] (
    [d_mms_ACH_charge_back_detail_id]           BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)       NOT NULL,
    [fact_mms_ach_charge_back_detail_key]       CHAR (32)       NULL,
    [charge_back_mms_tran_id]                   INT             NULL,
    [club_name]                                 VARCHAR (50)    NULL,
    [dim_mms_ach_charge_back_detail_member_key] CHAR (32)       NULL,
    [local_currency_code]                       VARCHAR (3)     NULL,
    [member_id]                                 INT             NULL,
    [posted_date]                               VARCHAR (50)    NULL,
    [reporting_currency_code]                   VARCHAR (3)     NULL,
    [transaction_amount]                        DECIMAL (26, 6) NULL,
    [transaction_date]                          VARCHAR (50)    NULL,
    [transaction_line_amount]                   DECIMAL (26, 6) NULL,
    [p_mms_ACH_charge_back_detail_id]           BIGINT          NOT NULL,
    [deleted_flag]                              INT             NULL,
    [dv_load_date_time]                         DATETIME        NULL,
    [dv_load_end_date_time]                     DATETIME        NULL,
    [dv_batch_id]                               BIGINT          NOT NULL,
    [dv_inserted_date_time]                     DATETIME        NOT NULL,
    [dv_insert_user]                            VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                      DATETIME        NULL,
    [dv_update_user]                            VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_ACH_charge_back_detail]([dv_batch_id] ASC);

