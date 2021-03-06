﻿CREATE TABLE [dbo].[stage_hash_lt_bucks_ProductOptions] (
    [stage_hash_lt_bucks_ProductOptions_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [poption_id]                            INT             NULL,
    [poption_product]                       INT             NULL,
    [poption_title]                         NVARCHAR (50)   NULL,
    [poption_price]                         DECIMAL (4)     NULL,
    [poption_active]                        BIT             NULL,
    [poption_timestamp]                     DATETIME        NULL,
    [poption_desc]                          NVARCHAR (2000) NULL,
    [poption_mms_id]                        INT             NULL,
    [poption_mms_multiplier]                INT             NULL,
    [poption_conf_email_addr]               VARCHAR (40)    NULL,
    [poption_expiration_days]               INT             NULL,
    [poption_has_quantities]                BIT             NULL,
    [poption_was_price]                     INT             NULL,
    [poption_continuousSchedule]            BIT             NULL,
    [LastModifiedTimestamp]                 DATETIME        NULL,
    [dv_load_date_time]                     DATETIME        NOT NULL,
    [dv_inserted_date_time]                 DATETIME        NOT NULL,
    [dv_insert_user]                        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                  DATETIME        NULL,
    [dv_update_user]                        VARCHAR (50)    NULL,
    [dv_batch_id]                           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

