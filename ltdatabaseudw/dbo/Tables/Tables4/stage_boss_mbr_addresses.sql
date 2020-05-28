CREATE TABLE [dbo].[stage_boss_mbr_addresses] (
    [stage_boss_mbr_addresses_id] BIGINT       NOT NULL,
    [id]                          INT          NULL,
    [line_1]                      VARCHAR (40) NULL,
    [line_2]                      VARCHAR (40) NULL,
    [city]                        VARCHAR (40) NULL,
    [zip]                         VARCHAR (5)  NULL,
    [zip_four]                    VARCHAR (4)  NULL,
    [state_code]                  VARCHAR (2)  NULL,
    [addr_type]                   VARCHAR (1)  NULL,
    [contact_id]                  INT          NULL,
    [created_at]                  DATETIME     NULL,
    [updated_at]                  DATETIME     NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

