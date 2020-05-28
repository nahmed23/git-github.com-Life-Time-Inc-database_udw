﻿CREATE TABLE [dbo].[stage_hash_lt_bucks_Users] (
    [stage_hash_lt_bucks_Users_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [user_id]                      INT            NULL,
    [user_username]                NVARCHAR (50)  NULL,
    [user_pass]                    NVARCHAR (50)  NULL,
    [user_fname]                   NVARCHAR (50)  NULL,
    [user_lname]                   NVARCHAR (50)  NULL,
    [user_email]                   NVARCHAR (50)  NULL,
    [user_phone]                   NVARCHAR (50)  NULL,
    [user_fax]                     NVARCHAR (50)  NULL,
    [user_taxid]                   NVARCHAR (15)  NULL,
    [user_birthdate]               DATETIME       NULL,
    [user_job_title]               NVARCHAR (75)  NULL,
    [user_business_name]           NVARCHAR (75)  NULL,
    [user_addr1]                   NVARCHAR (75)  NULL,
    [user_addr2]                   NVARCHAR (75)  NULL,
    [user_city]                    NVARCHAR (50)  NULL,
    [user_state]                   NVARCHAR (50)  NULL,
    [user_zip]                     NVARCHAR (15)  NULL,
    [user_language]                NVARCHAR (10)  NULL,
    [user_country]                 INT            NULL,
    [user_type]                    INT            NULL,
    [user_register_date]           SMALLDATETIME  NULL,
    [user_pending_points]          INT            NULL,
    [user_curr_points]             INT            NULL,
    [user_parent]                  INT            NULL,
    [user_dist_id]                 VARCHAR (9)    NULL,
    [user_promotion]               INT            NULL,
    [user_ref1]                    INT            NULL,
    [user_ref2]                    INT            NULL,
    [user_ref3]                    INT            NULL,
    [user_ref4]                    INT            NULL,
    [user_ref5]                    NVARCHAR (50)  NULL,
    [user_optout]                  BIT            NULL,
    [user_gender]                  NCHAR (1)      NULL,
    [user_web_addr]                NVARCHAR (200) NULL,
    [user_test]                    BIT            NULL,
    [user_active]                  BIT            NULL,
    [dv_load_date_time]            DATETIME       NOT NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));
