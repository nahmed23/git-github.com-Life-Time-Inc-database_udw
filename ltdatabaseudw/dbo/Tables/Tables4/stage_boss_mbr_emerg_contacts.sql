CREATE TABLE [dbo].[stage_boss_mbr_emerg_contacts] (
    [stage_boss_mbr_emerg_contacts_id] BIGINT       NOT NULL,
    [id]                               INT          NULL,
    [cust_code]                        VARCHAR (10) NULL,
    [mbr_code]                         VARCHAR (10) NULL,
    [created_at]                       DATETIME     NULL,
    [updated_at]                       DATETIME     NULL,
    [dv_batch_id]                      BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

