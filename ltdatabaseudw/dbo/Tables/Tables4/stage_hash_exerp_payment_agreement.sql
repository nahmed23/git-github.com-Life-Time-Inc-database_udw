CREATE TABLE [dbo].[stage_hash_exerp_payment_agreement] (
    [stage_hash_exerp_payment_agreement_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)      NOT NULL,
    [id]                                    VARCHAR (4000) NULL,
    [person_id]                             VARCHAR (4000) NULL,
    [clearinghouse]                         VARCHAR (4000) NULL,
    [refno]                                 VARCHAR (4000) NULL,
    [state]                                 VARCHAR (4000) NULL,
    [individual_deduction_day]              INT            NULL,
    [expire_date]                           DATETIME       NULL,
    [active]                                INT            NULL,
    [center_id]                             INT            NULL,
    [ets]                                   BIGINT         NULL,
    [dummy_modified_date_time]              DATETIME       NULL,
    [dv_load_date_time]                     DATETIME       NOT NULL,
    [dv_batch_id]                           BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

