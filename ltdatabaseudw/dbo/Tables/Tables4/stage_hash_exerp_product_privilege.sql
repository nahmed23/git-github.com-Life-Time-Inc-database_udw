CREATE TABLE [dbo].[stage_hash_exerp_product_privilege] (
    [stage_hash_exerp_product_privilege_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)       NOT NULL,
    [id]                                    INT             NULL,
    [privilege_set_id]                      INT             NULL,
    [price_mod_type]                        VARCHAR (4000)  NULL,
    [price_mod_value]                       DECIMAL (26, 6) NULL,
    [disable_min_price]                     BIT             NULL,
    [grant_purchase]                        BIT             NULL,
    [ref_type]                              VARCHAR (4000)  NULL,
    [ref_id]                                INT             NULL,
    [product_type]                          VARCHAR (4000)  NULL,
    [apply_type]                            VARCHAR (4000)  NULL,
    [apply_ref_type]                        VARCHAR (4000)  NULL,
    [apply_ref_id]                          INT             NULL,
    [relative_expansion]                    INT             NULL,
    [dummy_modified_date_time]              DATETIME        NULL,
    [dv_load_date_time]                     DATETIME        NOT NULL,
    [dv_batch_id]                           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

