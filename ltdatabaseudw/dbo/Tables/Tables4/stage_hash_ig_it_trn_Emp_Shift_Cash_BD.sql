CREATE TABLE [dbo].[stage_hash_ig_it_trn_Emp_Shift_Cash_BD] (
    [stage_hash_ig_it_trn_Emp_Shift_Cash_BD_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)       NOT NULL,
    [breakage_amt]                              DECIMAL (26, 6) NULL,
    [bus_day_id]                                INT             NULL,
    [cash_drop_amt]                             DECIMAL (26, 6) NULL,
    [cash_shift_id]                             INT             NULL,
    [change_amt]                                DECIMAL (26, 6) NULL,
    [emp_id]                                    INT             NULL,
    [loan_amt]                                  DECIMAL (26, 6) NULL,
    [num_tendered_checks]                       INT             NULL,
    [paidout_amt]                               DECIMAL (26, 6) NULL,
    [received_curr_amt]                         DECIMAL (26, 6) NULL,
    [tender_amt]                                DECIMAL (26, 6) NULL,
    [tender_id]                                 INT             NULL,
    [tender_qty]                                INT             NULL,
    [withdrawal_amt]                            DECIMAL (26, 6) NULL,
    [BD_start_dttime]                           DATETIME        NULL,
    [BD_end_dttime]                             DATETIME        NULL,
    [dummy_modified_date_time]                  DATETIME        NULL,
    [dv_load_date_time]                         DATETIME        NOT NULL,
    [dv_batch_id]                               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

