CREATE TABLE [dbo].[d_hybris_cat_2_prodrel] (
    [d_hybris_cat_2_prodrel_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)     NOT NULL,
    [d_hybris_cat_2_prodrel_key] CHAR (32)     NULL,
    [acl_ts]                     INT           NULL,
    [cat_2_prodrel_pk]           BIGINT        NULL,
    [created_ts]                 DATETIME      NULL,
    [hjmpts]                     BIGINT        NULL,
    [language_pk]                BIGINT        NULL,
    [modified_ts]                DATETIME      NULL,
    [Owner_Pk_String]            BIGINT        NULL,
    [prop_ts]                    INT           NULL,
    [qualifier]                  VARCHAR (255) NULL,
    [r_sequence_number]          INT           NULL,
    [sequence_number]            INT           NULL,
    [source_pk]                  BIGINT        NULL,
    [target_pk]                  BIGINT        NULL,
    [type_pk_string]             BIGINT        NULL,
    [p_hybris_cat_2_prodrel_id]  BIGINT        NOT NULL,
    [dv_load_date_time]          DATETIME      NULL,
    [dv_load_end_date_time]      DATETIME      NULL,
    [dv_batch_id]                BIGINT        NOT NULL,
    [dv_inserted_date_time]      DATETIME      NOT NULL,
    [dv_insert_user]             VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]       DATETIME      NULL,
    [dv_update_user]             VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

