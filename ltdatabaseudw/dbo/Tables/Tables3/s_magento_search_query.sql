CREATE TABLE [dbo].[s_magento_search_query] (
    [s_magento_search_query_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)     NOT NULL,
    [query_id]                  INT           NULL,
    [query_text]                VARCHAR (255) NULL,
    [num_results]               INT           NULL,
    [popularity]                INT           NULL,
    [redirect]                  VARCHAR (255) NULL,
    [display_in_terms]          INT           NULL,
    [is_active]                 INT           NULL,
    [is_processed]              INT           NULL,
    [updated_at]                DATETIME      NULL,
    [dv_load_date_time]         DATETIME      NOT NULL,
    [dv_r_load_source_id]       BIGINT        NOT NULL,
    [dv_inserted_date_time]     DATETIME      NOT NULL,
    [dv_insert_user]            VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]      DATETIME      NULL,
    [dv_update_user]            VARCHAR (50)  NULL,
    [dv_hash]                   CHAR (32)     NOT NULL,
    [dv_deleted]                BIT           DEFAULT ((0)) NOT NULL,
    [dv_batch_id]               BIGINT        NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

