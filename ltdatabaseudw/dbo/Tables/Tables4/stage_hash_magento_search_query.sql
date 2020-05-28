CREATE TABLE [dbo].[stage_hash_magento_search_query] (
    [stage_hash_magento_search_query_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)     NOT NULL,
    [query_id]                           INT           NULL,
    [query_text]                         VARCHAR (255) NULL,
    [num_results]                        INT           NULL,
    [popularity]                         INT           NULL,
    [redirect]                           VARCHAR (255) NULL,
    [store_id]                           INT           NULL,
    [display_in_terms]                   INT           NULL,
    [is_active]                          INT           NULL,
    [is_processed]                       INT           NULL,
    [updated_at]                         DATETIME      NULL,
    [dv_load_date_time]                  DATETIME      NOT NULL,
    [dv_batch_id]                        BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

