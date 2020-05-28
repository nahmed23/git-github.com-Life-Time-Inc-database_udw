CREATE TABLE [dbo].[stage_magento_eav_attribute] (
    [stage_magento_eav_attribute_id] BIGINT         NOT NULL,
    [attribute_id]                   INT            NULL,
    [entity_type_id]                 INT            NULL,
    [attribute_code]                 VARCHAR (255)  NULL,
    [attribute_model]                VARCHAR (255)  NULL,
    [backend_model]                  VARCHAR (255)  NULL,
    [backend_type]                   VARCHAR (8)    NULL,
    [backend_table]                  VARCHAR (255)  NULL,
    [frontend_model]                 VARCHAR (255)  NULL,
    [frontend_input]                 VARCHAR (50)   NULL,
    [frontend_label]                 VARCHAR (255)  NULL,
    [frontend_class]                 VARCHAR (255)  NULL,
    [source_model]                   VARCHAR (255)  NULL,
    [is_required]                    INT            NULL,
    [is_user_defined]                INT            NULL,
    [default_value]                  VARCHAR (8000) NULL,
    [is_unique]                      INT            NULL,
    [note]                           VARCHAR (255)  NULL,
    [dummy_modified_date_time]       DATETIME       NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

