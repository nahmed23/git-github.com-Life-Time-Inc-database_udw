CREATE TABLE [dbo].[s_hybris_users] (
    [s_hybris_users_id]             BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                       CHAR (32)      NOT NULL,
    [hjmpts]                        BIGINT         NULL,
    [created_ts]                    DATETIME       NULL,
    [modified_ts]                   DATETIME       NULL,
    [users_pk]                      BIGINT         NULL,
    [p_description]                 NVARCHAR (255) NULL,
    [p_name]                        NVARCHAR (255) NULL,
    [p_uid]                         NVARCHAR (255) NULL,
    [p_back_office_log_in_disabled] TINYINT        NULL,
    [p_ldap_search_base]            NVARCHAR (255) NULL,
    [p_dn]                          VARCHAR (8000) NULL,
    [p_cn]                          NVARCHAR (255) NULL,
    [p_login_disabled]              TINYINT        NULL,
    [p_last_login]                  DATETIME       NULL,
    [p_hmc_log_in_disabled]         TINYINT        NULL,
    [p_ldap_account]                TINYINT        NULL,
    [p_domain]                      NVARCHAR (255) NULL,
    [p_ldap_log_in]                 NVARCHAR (255) NULL,
    [p_authorized_to_unlock_pages]  TINYINT        NULL,
    [p_ltf_party_id]                INT            NULL,
    [p_member_id]                   INT            NULL,
    [p_membership_id]               INT            NULL,
    [acl_ts]                        BIGINT         NULL,
    [prop_ts]                       BIGINT         NULL,
    [p_customer_id]                 NVARCHAR (255) NULL,
    [p_preview_catalog_versions]    VARCHAR (8000) NULL,
    [p_token]                       VARCHAR (8000) NULL,
    [p_original_uid]                NVARCHAR (255) NULL,
    [dv_load_date_time]             DATETIME       NOT NULL,
    [dv_r_load_source_id]           BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL,
    [dv_hash]                       CHAR (32)      NOT NULL,
    [dv_batch_id]                   BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_users]
    ON [dbo].[s_hybris_users]([bk_hash] ASC, [s_hybris_users_id] ASC);

