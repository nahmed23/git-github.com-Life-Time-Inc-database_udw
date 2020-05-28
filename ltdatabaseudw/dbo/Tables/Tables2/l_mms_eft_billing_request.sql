CREATE TABLE [dbo].[l_mms_eft_billing_request] (
    [l_mms_eft_billing_request_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [eft_billing_request_id]       INT          NULL,
    [club_id]                      VARCHAR (20) NULL,
    [person_id]                    VARCHAR (20) NULL,
    [product_id]                   VARCHAR (20) NULL,
    [external_item_id]             VARCHAR (20) NULL,
    [external_package_id]          VARCHAR (50) NULL,
    [original_external_item_id]    VARCHAR (20) NULL,
    [subscription_id]              VARCHAR (50) NULL,
    [mms_tran_id]                  INT          NULL,
    [package_id]                   INT          NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_r_load_source_id]          BIGINT       NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_hash]                      CHAR (32)    NOT NULL,
    [dv_deleted]                   BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

