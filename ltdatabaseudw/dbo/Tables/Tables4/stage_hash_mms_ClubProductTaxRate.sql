CREATE TABLE [dbo].[stage_hash_mms_ClubProductTaxRate] (
    [stage_hash_mms_ClubProductTaxRate_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [ClubProductTaxRateID]                 INT          NULL,
    [ClubID]                               INT          NULL,
    [ProductID]                            INT          NULL,
    [TaxRateID]                            SMALLINT     NULL,
    [StartDate]                            DATETIME     NULL,
    [EndDate]                              DATETIME     NULL,
    [InsertedDatetime]                     DATETIME     NULL,
    [UpdatedDateTime]                      DATETIME     NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

