CREATE TABLE [dbo].[s_exerp_payment_agreement] (
    [s_exerp_payment_agreement_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [payment_agreement_id]         VARCHAR (4000) NULL,
    [clearing_house]               VARCHAR (4000) NULL,
    [ref_no]                       VARCHAR (4000) NULL,
    [state]                        VARCHAR (4000) NULL,
    [individual_deduction_day]     INT            NULL,
    [expire_date]                  DATETIME       NULL,
    [active]                       INT            NULL,
    [ets]                          BIGINT         NULL,
    [dummy_modified_date_time]     DATETIME       NULL,
    [dv_load_date_time]            DATETIME       NOT NULL,
    [dv_r_load_source_id]          BIGINT         NOT NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL,
    [dv_hash]                      CHAR (32)      NOT NULL,
    [dv_deleted]                   BIT            DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

