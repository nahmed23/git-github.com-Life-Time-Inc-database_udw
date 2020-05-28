CREATE TABLE [dbo].[stage_magento_catalogrule] (
    [stage_magento_catalogrule_id] BIGINT          NOT NULL,
    [row_id]                       INT             NULL,
    [rule_id]                      INT             NULL,
    [created_in]                   BIGINT          NULL,
    [updated_in]                   BIGINT          NULL,
    [name]                         VARCHAR (255)   NULL,
    [description]                  VARCHAR (8000)  NULL,
    [from_date]                    DATE            NULL,
    [to_date]                      DATE            NULL,
    [is_active]                    INT             NULL,
    [conditions_serialized]        VARCHAR (8000)  NULL,
    [actions_serialized]           VARCHAR (8000)  NULL,
    [stop_rules_processing]        INT             NULL,
    [sort_order]                   INT             NULL,
    [simple_action]                VARCHAR (32)    NULL,
    [discount_amount]              DECIMAL (12, 4) NULL,
    [dummy_modified_date_time]     DATETIME        NULL,
    [dv_batch_id]                  BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

