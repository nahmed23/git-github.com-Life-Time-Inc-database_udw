CREATE TABLE [dbo].[fact_orbital_transaction_data_report] (
    [fact_orbital_transaction_data_report_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [fact_orbital_transaction_data_report_key] CHAR (32)       NULL,
    [Amount]                                   DECIMAL (10, 2) NULL,
    [category_id]                              VARCHAR (50)    NULL,
    [company_id]                               VARCHAR (20)    NULL,
    [cost_center_id]                           VARCHAR (20)    NULL,
    [currency_id]                              VARCHAR (20)    NULL,
    [deposit]                                  VARCHAR (10)    NULL,
    [Merchant_Number]                          VARCHAR (20)    NULL,
    [Posted_Date]                              VARCHAR (29)    NULL,
    [region_id]                                VARCHAR (10)    NULL,
    [tender_type_id]                           VARCHAR (20)    NULL,
    [Transaction_Date]                         DATE            NULL,
    [Transaction_Id]                           VARCHAR (50)    NULL,
    [withdrawl]                                VARCHAR (10)    NULL,
    [dv_load_date_time]                        DATETIME        NULL,
    [dv_load_end_date_time]                    DATETIME        NULL,
    [dv_batch_id]                              BIGINT          NOT NULL,
    [dv_inserted_date_time]                    DATETIME        NOT NULL,
    [dv_insert_user]                           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                     DATETIME        NULL,
    [dv_update_user]                           VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_orbital_transaction_data_report_key]));

