﻿CREATE TABLE [dbo].[l_mms_membership_1] (
    [l_mms_membership_1_id]     BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)    NOT NULL,
    [membership_id]             INT          NULL,
    [crm_account_id]            VARCHAR (36) NULL,
    [val_eft_option_product_id] TINYINT      NULL,
    [dv_load_date_time]         DATETIME     NOT NULL,
    [dv_r_load_source_id]       BIGINT       NOT NULL,
    [dv_inserted_date_time]     DATETIME     NOT NULL,
    [dv_insert_user]            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]      DATETIME     NULL,
    [dv_update_user]            VARCHAR (50) NULL,
    [dv_hash]                   CHAR (32)    NOT NULL,
    [dv_deleted]                BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]               BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

