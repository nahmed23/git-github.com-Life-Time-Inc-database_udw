CREATE TABLE [dbo].[d_orbital_transaction_data_report] (
    [d_orbital_transaction_data_report_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)       NOT NULL,
    [fact_orbital_transaction_data_report_key] CHAR (32)       NULL,
    [Merchant_Number]                          VARCHAR (20)    NULL,
    [Batch_Number]                             VARCHAR (15)    NULL,
    [Transaction_Id]                           VARCHAR (15)    NULL,
    [Amount]                                   DECIMAL (10, 2) NULL,
    [deposit_flag]                             VARCHAR (3)     NULL,
    [MOP_Code]                                 VARCHAR (2)     NULL,
    [tender_type_id]                           VARCHAR (10)    NULL,
    [Transaction_Date]                         VARCHAR (29)    NULL,
    [p_orbital_transaction_data_report_id]     BIGINT          NOT NULL,
    [deleted_flag]                             INT             NULL,
    [dv_load_date_time]                        DATETIME        NULL,
    [dv_load_end_date_time]                    DATETIME        NULL,
    [dv_batch_id]                              BIGINT          NOT NULL,
    [dv_inserted_date_time]                    DATETIME        NOT NULL,
    [dv_insert_user]                           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                     DATETIME        NULL,
    [dv_update_user]                           VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

