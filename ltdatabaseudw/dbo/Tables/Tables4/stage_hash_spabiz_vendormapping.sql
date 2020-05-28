CREATE TABLE [dbo].[stage_hash_spabiz_vendormapping] (
    [stage_hash_spabiz_vendormapping_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)    NOT NULL,
    [idvendormapping]                    BIGINT       NULL,
    [spabiz_vendordatabaseid]            BIGINT       NULL,
    [workday_supplierid]                 BIGINT       NULL,
    [jan_one]                            DATETIME     NULL,
    [dv_load_date_time]                  DATETIME     NOT NULL,
    [dv_inserted_date_time]              DATETIME     NOT NULL,
    [dv_insert_user]                     VARCHAR (50) NOT NULL,
    [dv_updated_date_time]               DATETIME     NULL,
    [dv_update_user]                     VARCHAR (50) NULL,
    [dv_batch_id]                        BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

