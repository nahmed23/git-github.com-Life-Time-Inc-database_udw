CREATE TABLE [dbo].[stage_exerp_payment_agreement] (
    [stage_exerp_payment_agreement_id] BIGINT         NOT NULL,
    [id]                               VARCHAR (4000) NULL,
    [person_id]                        VARCHAR (4000) NULL,
    [clearinghouse]                    VARCHAR (4000) NULL,
    [refno]                            VARCHAR (4000) NULL,
    [state]                            VARCHAR (4000) NULL,
    [individual_deduction_day]         INT            NULL,
    [expire_date]                      DATETIME       NULL,
    [active]                           INT            NULL,
    [center_id]                        INT            NULL,
    [ets]                              BIGINT         NULL,
    [dummy_modified_date_time]         DATETIME       NULL,
    [dv_batch_id]                      BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

