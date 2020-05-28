CREATE PROC [dbo].[proc_etl_hybris_affiliate_entry_details] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_hybris_affiliateentrydetails

set @insert_date_time = getdate()
insert into dbo.stage_hash_hybris_affiliateentrydetails (
       bk_hash,
       hjmpTS,
       TypePkString,
       PK,
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       ltfemployeeid,
       ltfaffvalendtime,
       ltfpartyid,
       ltfaffiliateid,
       ltfpurchaseflag,
       ltfaffvalstarttime,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(PK as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       hjmpTS,
       TypePkString,
       PK,
       createdTS,
       modifiedTS,
       OwnerPkString,
       aCLTS,
       propTS,
       ltfemployeeid,
       ltfaffvalendtime,
       ltfpartyid,
       ltfaffiliateid,
       ltfpurchaseflag,
       ltfaffvalstarttime,
       isnull(cast(stage_hybris_affiliateentrydetails.modifiedts as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_hybris_affiliateentrydetails
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_hybris_affiliate_entry_details @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_hybris_affiliate_entry_details (
       bk_hash,
       affiliate_entry_details_pk,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_hybris_affiliateentrydetails.bk_hash,
       stage_hash_hybris_affiliateentrydetails.PK affiliate_entry_details_pk,
       isnull(cast(stage_hash_hybris_affiliateentrydetails.modifiedts as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       12,
       @insert_date_time,
       @user
  from stage_hash_hybris_affiliateentrydetails
  left join h_hybris_affiliate_entry_details
    on stage_hash_hybris_affiliateentrydetails.bk_hash = h_hybris_affiliate_entry_details.bk_hash
 where h_hybris_affiliate_entry_details_id is null
   and stage_hash_hybris_affiliateentrydetails.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_hybris_affiliate_entry_details
if object_id('tempdb..#l_hybris_affiliate_entry_details_inserts') is not null drop table #l_hybris_affiliate_entry_details_inserts
create table #l_hybris_affiliate_entry_details_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_affiliateentrydetails.bk_hash,
       stage_hash_hybris_affiliateentrydetails.TypePkString type_pk_string,
       stage_hash_hybris_affiliateentrydetails.PK affiliate_entry_details_pk,
       stage_hash_hybris_affiliateentrydetails.OwnerPkString owner_pk_string,
       stage_hash_hybris_affiliateentrydetails.ltfemployeeid ltf_employee_id,
       stage_hash_hybris_affiliateentrydetails.ltfpartyid ltf_party_id,
       stage_hash_hybris_affiliateentrydetails.ltfaffiliateid ltf_affiliate_id,
       isnull(cast(stage_hash_hybris_affiliateentrydetails.modifiedts as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_affiliateentrydetails.TypePkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_affiliateentrydetails.PK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_affiliateentrydetails.OwnerPkString as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_affiliateentrydetails.ltfemployeeid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_affiliateentrydetails.ltfpartyid,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_hybris_affiliateentrydetails.ltfaffiliateid,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_affiliateentrydetails
 where stage_hash_hybris_affiliateentrydetails.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_hybris_affiliate_entry_details records
set @insert_date_time = getdate()
insert into l_hybris_affiliate_entry_details (
       bk_hash,
       type_pk_string,
       affiliate_entry_details_pk,
       owner_pk_string,
       ltf_employee_id,
       ltf_party_id,
       ltf_affiliate_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_hybris_affiliate_entry_details_inserts.bk_hash,
       #l_hybris_affiliate_entry_details_inserts.type_pk_string,
       #l_hybris_affiliate_entry_details_inserts.affiliate_entry_details_pk,
       #l_hybris_affiliate_entry_details_inserts.owner_pk_string,
       #l_hybris_affiliate_entry_details_inserts.ltf_employee_id,
       #l_hybris_affiliate_entry_details_inserts.ltf_party_id,
       #l_hybris_affiliate_entry_details_inserts.ltf_affiliate_id,
       case when l_hybris_affiliate_entry_details.l_hybris_affiliate_entry_details_id is null then isnull(#l_hybris_affiliate_entry_details_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #l_hybris_affiliate_entry_details_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_hybris_affiliate_entry_details_inserts
  left join p_hybris_affiliate_entry_details
    on #l_hybris_affiliate_entry_details_inserts.bk_hash = p_hybris_affiliate_entry_details.bk_hash
   and p_hybris_affiliate_entry_details.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_hybris_affiliate_entry_details
    on p_hybris_affiliate_entry_details.bk_hash = l_hybris_affiliate_entry_details.bk_hash
   and p_hybris_affiliate_entry_details.l_hybris_affiliate_entry_details_id = l_hybris_affiliate_entry_details.l_hybris_affiliate_entry_details_id
 where l_hybris_affiliate_entry_details.l_hybris_affiliate_entry_details_id is null
    or (l_hybris_affiliate_entry_details.l_hybris_affiliate_entry_details_id is not null
        and l_hybris_affiliate_entry_details.dv_hash <> #l_hybris_affiliate_entry_details_inserts.source_hash)

--calculate hash and lookup to current s_hybris_affiliate_entry_details
if object_id('tempdb..#s_hybris_affiliate_entry_details_inserts') is not null drop table #s_hybris_affiliate_entry_details_inserts
create table #s_hybris_affiliate_entry_details_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_hybris_affiliateentrydetails.bk_hash,
       stage_hash_hybris_affiliateentrydetails.hjmpTS hjmpts,
       stage_hash_hybris_affiliateentrydetails.PK affiliate_entry_details_pk,
       stage_hash_hybris_affiliateentrydetails.createdTS created_ts,
       stage_hash_hybris_affiliateentrydetails.modifiedTS modified_ts,
       stage_hash_hybris_affiliateentrydetails.aCLTS acl_ts,
       stage_hash_hybris_affiliateentrydetails.propTS prop_ts,
       stage_hash_hybris_affiliateentrydetails.ltfaffvalendtime ltf_affval_end_time,
       stage_hash_hybris_affiliateentrydetails.ltfpurchaseflag ltf_purchase_flag,
       stage_hash_hybris_affiliateentrydetails.ltfaffvalstarttime ltf_aff_val_start_time,
       isnull(cast(stage_hash_hybris_affiliateentrydetails.modifiedts as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_hybris_affiliateentrydetails.hjmpTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_affiliateentrydetails.PK as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_affiliateentrydetails.createdTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_affiliateentrydetails.modifiedTS,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_affiliateentrydetails.aCLTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_affiliateentrydetails.propTS as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_affiliateentrydetails.ltfaffvalendtime,120),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(cast(stage_hash_hybris_affiliateentrydetails.ltfpurchaseflag as varchar(500)),'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_hybris_affiliateentrydetails.ltfaffvalstarttime,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_hybris_affiliateentrydetails
 where stage_hash_hybris_affiliateentrydetails.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_hybris_affiliate_entry_details records
set @insert_date_time = getdate()
insert into s_hybris_affiliate_entry_details (
       bk_hash,
       hjmpts,
       affiliate_entry_details_pk,
       created_ts,
       modified_ts,
       acl_ts,
       prop_ts,
       ltf_affval_end_time,
       ltf_purchase_flag,
       ltf_aff_val_start_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_hybris_affiliate_entry_details_inserts.bk_hash,
       #s_hybris_affiliate_entry_details_inserts.hjmpts,
       #s_hybris_affiliate_entry_details_inserts.affiliate_entry_details_pk,
       #s_hybris_affiliate_entry_details_inserts.created_ts,
       #s_hybris_affiliate_entry_details_inserts.modified_ts,
       #s_hybris_affiliate_entry_details_inserts.acl_ts,
       #s_hybris_affiliate_entry_details_inserts.prop_ts,
       #s_hybris_affiliate_entry_details_inserts.ltf_affval_end_time,
       #s_hybris_affiliate_entry_details_inserts.ltf_purchase_flag,
       #s_hybris_affiliate_entry_details_inserts.ltf_aff_val_start_time,
       case when s_hybris_affiliate_entry_details.s_hybris_affiliate_entry_details_id is null then isnull(#s_hybris_affiliate_entry_details_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       12,
       #s_hybris_affiliate_entry_details_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_hybris_affiliate_entry_details_inserts
  left join p_hybris_affiliate_entry_details
    on #s_hybris_affiliate_entry_details_inserts.bk_hash = p_hybris_affiliate_entry_details.bk_hash
   and p_hybris_affiliate_entry_details.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_hybris_affiliate_entry_details
    on p_hybris_affiliate_entry_details.bk_hash = s_hybris_affiliate_entry_details.bk_hash
   and p_hybris_affiliate_entry_details.s_hybris_affiliate_entry_details_id = s_hybris_affiliate_entry_details.s_hybris_affiliate_entry_details_id
 where s_hybris_affiliate_entry_details.s_hybris_affiliate_entry_details_id is null
    or (s_hybris_affiliate_entry_details.s_hybris_affiliate_entry_details_id is not null
        and s_hybris_affiliate_entry_details.dv_hash <> #s_hybris_affiliate_entry_details_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_hybris_affiliate_entry_details @current_dv_batch_id

end
