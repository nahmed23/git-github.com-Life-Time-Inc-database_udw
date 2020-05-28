﻿CREATE TABLE [dbo].[dim_exerp_clipcard] (
    [dim_exerp_clipcard_id]          BIGINT         IDENTITY (1, 1) NOT NULL,
    [assigned_dim_employee_key]      VARCHAR (32)   NULL,
    [blocked_flag]                   CHAR (1)       NULL,
    [cancel_dim_date_key]            VARCHAR (8)    NULL,
    [cancel_dim_time_key]            INT            NULL,
    [cancelled_flag]                 CHAR (1)       NULL,
    [clipcard_id]                    VARCHAR (4000) NULL,
    [clips_initial]                  INT            NULL,
    [clips_left]                     INT            NULL,
    [comment]                        VARCHAR (4000) NULL,
    [dim_club_key]                   VARCHAR (32)   NULL,
    [dim_exerp_clipcard_key]         VARCHAR (32)   NULL,
    [dim_exerp_product_key]          VARCHAR (32)   NULL,
    [dim_mms_member_key]             VARCHAR (32)   NULL,
    [fact_exerp_transaction_log_key] VARCHAR (32)   NULL,
    [sale_entered_dim_employee_key]  VARCHAR (32)   NULL,
    [sale_entry_dim_date_key]        VARCHAR (8)    NULL,
    [sale_entry_dim_time_key]        INT            NULL,
    [sale_source_type]               VARCHAR (4000) NULL,
    [valid_from_dim_date_key]        VARCHAR (8)    NULL,
    [valid_from_dim_time_key]        INT            NULL,
    [valid_until_dim_date_key]       VARCHAR (8)    NULL,
    [valid_until_dim_time_key]       INT            NULL,
    [dv_load_date_time]              DATETIME       NULL,
    [dv_load_end_date_time]          DATETIME       NULL,
    [dv_batch_id]                    BIGINT         NOT NULL,
    [dv_inserted_date_time]          DATETIME       NOT NULL,
    [dv_insert_user]                 VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]           DATETIME       NULL,
    [dv_update_user]                 VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_exerp_clipcard_key]));
