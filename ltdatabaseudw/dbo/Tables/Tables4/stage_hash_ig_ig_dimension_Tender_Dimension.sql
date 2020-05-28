CREATE TABLE [dbo].[stage_hash_ig_ig_dimension_Tender_Dimension] (
    [stage_hash_ig_ig_dimension_Tender_Dimension_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                        CHAR (32)     NOT NULL,
    [tender_dim_id]                                  BIGINT        NULL,
    [profit_center_dim_level2_id]                    INT           NULL,
    [tender_id]                                      INT           NULL,
    [tender_name]                                    NVARCHAR (50) NULL,
    [tender_class_id]                                INT           NULL,
    [tender_class_name]                              NVARCHAR (50) NULL,
    [cash_tender_flag]                               BIT           NULL,
    [comp_tender_flag]                               BIT           NULL,
    [eff_date_from]                                  DATETIME      NULL,
    [eff_date_to]                                    DATETIME      NULL,
    [customer_id]                                    INT           NULL,
    [ent_id]                                         INT           NULL,
    [corp_id]                                        INT           NULL,
    [additional_checkid_code_id]                     TINYINT       NULL,
    [dv_load_date_time]                              DATETIME      NOT NULL,
    [dv_batch_id]                                    BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

