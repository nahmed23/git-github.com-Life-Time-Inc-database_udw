﻿CREATE TABLE [dbo].[d_mms_member_history] (
    [d_mms_member_history_id]         BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)     NOT NULL,
    [dim_mms_member_key]              CHAR (32)     NULL,
    [member_id]                       INT           NULL,
    [effective_date_time]             DATETIME      NULL,
    [expiration_date_time]            DATETIME      NULL,
    [assess_junior_member_dues_flag]  CHAR (1)      NULL,
    [customer_name]                   VARCHAR (101) NULL,
    [customer_name_last_first]        VARCHAR (102) NULL,
    [date_of_birth]                   DATETIME      NULL,
    [description_member]              VARCHAR (50)  NULL,
    [dim_mms_membership_key]          CHAR (32)     NULL,
    [email_address]                   VARCHAR (140) NULL,
    [first_name]                      VARCHAR (50)  NULL,
    [gender_abbreviation]             VARCHAR (1)   NULL,
    [join_date]                       DATETIME      NULL,
    [last_name]                       VARCHAR (50)  NULL,
    [member_active_flag]              CHAR (1)      NULL,
    [member_type_dim_description_key] VARCHAR (532) NULL,
    [membership_id]                   INT           NULL,
    [val_member_type_id]              INT           NULL,
    [p_mms_member_id]                 BIGINT        NOT NULL,
    [dv_load_date_time]               DATETIME      NULL,
    [dv_load_end_date_time]           DATETIME      NULL,
    [dv_batch_id]                     BIGINT        NOT NULL,
    [dv_inserted_date_time]           DATETIME      NOT NULL,
    [dv_insert_user]                  VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]            DATETIME      NULL,
    [dv_update_user]                  VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_mms_member_history]([dv_batch_id] ASC);

