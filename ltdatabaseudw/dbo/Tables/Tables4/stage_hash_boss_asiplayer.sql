﻿CREATE TABLE [dbo].[stage_hash_boss_asiplayer] (
    [stage_hash_boss_asiplayer_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)       NOT NULL,
    [reservation]                  INT             NULL,
    [date_used]                    DATETIME        NULL,
    [cust_code]                    CHAR (10)       NULL,
    [mbr_code]                     CHAR (10)       NULL,
    [sequence]                     SMALLINT        NULL,
    [price]                        DECIMAL (26, 6) NULL,
    [tax_amt]                      DECIMAL (26, 6) NULL,
    [paid]                         CHAR (1)        NULL,
    [trans]                        INT             NULL,
    [instructor]                   CHAR (30)       NULL,
    [comm_paid]                    CHAR (1)        NULL,
    [employee_id]                  CHAR (10)       NULL,
    [phone]                        CHAR (10)       NULL,
    [player_name]                  CHAR (30)       NULL,
    [can_charge]                   CHAR (1)        NULL,
    [checked_in]                   CHAR (1)        NULL,
    [email]                        NCHAR (50)      NULL,
    [cancel_date]                  DATETIME        NULL,
    [notes]                        VARCHAR (240)   NULL,
    [status]                       CHAR (1)        NULL,
    [start_date]                   DATETIME        NULL,
    [origin]                       CHAR (1)        NULL,
    [DOB]                          DATETIME        NULL,
    [mbr_type]                     CHAR (1)        NULL,
    [house_acct]                   CHAR (10)       NULL,
    [created_at]                   DATETIME        NULL,
    [id]                           INT             NULL,
    [balance_due]                  DECIMAL (26, 6) NULL,
    [contact_id]                   INT             NULL,
    [mbrship_type_ID]              INT             NULL,
    [rostered_by]                  INT             NULL,
    [cust_type]                    INT             NULL,
    [mms_trans_id]                 INT             NULL,
    [updated_at]                   DATETIME        NULL,
    [pmt_start]                    DATETIME        NULL,
    [pmt_end]                      DATETIME        NULL,
    [recurrence_id]                INT             NULL,
    [check_in_date]                DATETIME        NULL,
    [last_paid_date]               DATETIME        NULL,
    [mms_swipe]                    CHAR (1)        NULL,
    [package_balance]              INT             NULL,
    [dv_load_date_time]            DATETIME        NOT NULL,
    [dv_inserted_date_time]        DATETIME        NOT NULL,
    [dv_insert_user]               VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]         DATETIME        NULL,
    [dv_update_user]               VARCHAR (50)    NULL,
    [dv_batch_id]                  BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

