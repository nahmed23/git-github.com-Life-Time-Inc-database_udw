CREATE TABLE [dbo].[stage_boss_mbr_med_details] (
    [stage_boss_mbr_med_details_id] BIGINT         NOT NULL,
    [id]                            INT            NULL,
    [cust_code]                     VARCHAR (10)   NULL,
    [mbr_code]                      VARCHAR (10)   NULL,
    [med_admin_auth]                INT            NULL,
    [immun_current]                 INT            NULL,
    [med_info]                      VARCHAR (4000) NULL,
    [allergy_info]                  VARCHAR (4000) NULL,
    [created_at]                    DATETIME       NULL,
    [updated_at]                    DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

