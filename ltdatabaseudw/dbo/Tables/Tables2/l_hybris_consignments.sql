CREATE TABLE [dbo].[l_hybris_consignments] (
    [l_hybris_consignments_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)    NOT NULL,
    [type_pk_string]              BIGINT       NULL,
    [consignments_pk]             BIGINT       NULL,
    [owner_pk_string]             BIGINT       NULL,
    [p_status]                    BIGINT       NULL,
    [p_warehouse]                 BIGINT       NULL,
    [p_shipping_address]          BIGINT       NULL,
    [p_order]                     BIGINT       NULL,
    [p_delivery_mode]             BIGINT       NULL,
    [p_delivery_point_of_service] BIGINT       NULL,
    [dv_load_date_time]           DATETIME     NOT NULL,
    [dv_r_load_source_id]         BIGINT       NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL,
    [dv_hash]                     CHAR (32)    NOT NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_consignments]
    ON [dbo].[l_hybris_consignments]([bk_hash] ASC, [l_hybris_consignments_id] ASC);

