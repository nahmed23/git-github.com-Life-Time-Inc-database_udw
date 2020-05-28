CREATE PROC [dbo].[proc_etl_exerp_product_privilege] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_exerp_product_privilege

set @insert_date_time = getdate()
insert into dbo.stage_hash_exerp_product_privilege (
       bk_hash,
       id,
       privilege_set_id,
       price_mod_type,
       price_mod_value,
       disable_min_price,
       grant_purchase,
       ref_type,
       ref_id,
       product_type,
       apply_type,
       apply_ref_type,
       apply_ref_id,
       relative_expansion,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       id,
       privilege_set_id,
       price_mod_type,
       price_mod_value,
       disable_min_price,
       grant_purchase,
       ref_type,
       ref_id,
       product_type,
       apply_type,
       apply_ref_type,
       apply_ref_id,
       relative_expansion,
       dummy_modified_date_time,
       isnull(cast(stage_exerp_product_privilege.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_exerp_product_privilege
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_exerp_product_privilege @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_exerp_product_privilege (
       bk_hash,
       product_privilege_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select distinct stage_hash_exerp_product_privilege.bk_hash,
       stage_hash_exerp_product_privilege.id product_privilege_id,
       isnull(cast(stage_hash_exerp_product_privilege.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       33,
       @insert_date_time,
       @user
  from stage_hash_exerp_product_privilege
  left join h_exerp_product_privilege
    on stage_hash_exerp_product_privilege.bk_hash = h_exerp_product_privilege.bk_hash
 where h_exerp_product_privilege_id is null
   and stage_hash_exerp_product_privilege.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_exerp_product_privilege
if object_id('tempdb..#l_exerp_product_privilege_inserts') is not null drop table #l_exerp_product_privilege_inserts
create table #l_exerp_product_privilege_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_product_privilege.bk_hash,
       stage_hash_exerp_product_privilege.id product_privilege_id,
       stage_hash_exerp_product_privilege.privilege_set_id privilege_set_id,
       stage_hash_exerp_product_privilege.ref_id ref_id,
       stage_hash_exerp_product_privilege.apply_ref_id apply_ref_id,
       isnull(cast(stage_hash_exerp_product_privilege.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_product_privilege.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product_privilege.privilege_set_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product_privilege.ref_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product_privilege.apply_ref_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_product_privilege
 where stage_hash_exerp_product_privilege.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_exerp_product_privilege records
set @insert_date_time = getdate()
insert into l_exerp_product_privilege (
       bk_hash,
       product_privilege_id,
       privilege_set_id,
       ref_id,
       apply_ref_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_exerp_product_privilege_inserts.bk_hash,
       #l_exerp_product_privilege_inserts.product_privilege_id,
       #l_exerp_product_privilege_inserts.privilege_set_id,
       #l_exerp_product_privilege_inserts.ref_id,
       #l_exerp_product_privilege_inserts.apply_ref_id,
       case when l_exerp_product_privilege.l_exerp_product_privilege_id is null then isnull(#l_exerp_product_privilege_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #l_exerp_product_privilege_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_exerp_product_privilege_inserts
  left join p_exerp_product_privilege
    on #l_exerp_product_privilege_inserts.bk_hash = p_exerp_product_privilege.bk_hash
   and p_exerp_product_privilege.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_exerp_product_privilege
    on p_exerp_product_privilege.bk_hash = l_exerp_product_privilege.bk_hash
   and p_exerp_product_privilege.l_exerp_product_privilege_id = l_exerp_product_privilege.l_exerp_product_privilege_id
 where l_exerp_product_privilege.l_exerp_product_privilege_id is null
    or (l_exerp_product_privilege.l_exerp_product_privilege_id is not null
        and l_exerp_product_privilege.dv_hash <> #l_exerp_product_privilege_inserts.source_hash)

--calculate hash and lookup to current s_exerp_product_privilege
if object_id('tempdb..#s_exerp_product_privilege_inserts') is not null drop table #s_exerp_product_privilege_inserts
create table #s_exerp_product_privilege_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_exerp_product_privilege.bk_hash,
       stage_hash_exerp_product_privilege.id product_privilege_id,
       stage_hash_exerp_product_privilege.price_mod_type price_mod_type,
       stage_hash_exerp_product_privilege.price_mod_value price_mod_value,
       stage_hash_exerp_product_privilege.disable_min_price disable_min_price,
       stage_hash_exerp_product_privilege.grant_purchase grant_purchase,
       stage_hash_exerp_product_privilege.ref_type ref_type,
       stage_hash_exerp_product_privilege.product_type product_type,
       stage_hash_exerp_product_privilege.apply_type apply_type,
       stage_hash_exerp_product_privilege.apply_ref_type apply_ref_type,
       stage_hash_exerp_product_privilege.relative_expansion relative_expansion,
       stage_hash_exerp_product_privilege.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_exerp_product_privilege.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_exerp_product_privilege.id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_product_privilege.price_mod_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product_privilege.price_mod_value as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product_privilege.disable_min_price as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product_privilege.grant_purchase as varchar(42)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_product_privilege.ref_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_product_privilege.product_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_product_privilege.apply_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_exerp_product_privilege.apply_ref_type,'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_exerp_product_privilege.relative_expansion as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_exerp_product_privilege
 where stage_hash_exerp_product_privilege.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_exerp_product_privilege records
set @insert_date_time = getdate()
insert into s_exerp_product_privilege (
       bk_hash,
       product_privilege_id,
       price_mod_type,
       price_mod_value,
       disable_min_price,
       grant_purchase,
       ref_type,
       product_type,
       apply_type,
       apply_ref_type,
       relative_expansion,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_exerp_product_privilege_inserts.bk_hash,
       #s_exerp_product_privilege_inserts.product_privilege_id,
       #s_exerp_product_privilege_inserts.price_mod_type,
       #s_exerp_product_privilege_inserts.price_mod_value,
       #s_exerp_product_privilege_inserts.disable_min_price,
       #s_exerp_product_privilege_inserts.grant_purchase,
       #s_exerp_product_privilege_inserts.ref_type,
       #s_exerp_product_privilege_inserts.product_type,
       #s_exerp_product_privilege_inserts.apply_type,
       #s_exerp_product_privilege_inserts.apply_ref_type,
       #s_exerp_product_privilege_inserts.relative_expansion,
       #s_exerp_product_privilege_inserts.dummy_modified_date_time,
       case when s_exerp_product_privilege.s_exerp_product_privilege_id is null then isnull(#s_exerp_product_privilege_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       33,
       #s_exerp_product_privilege_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_exerp_product_privilege_inserts
  left join p_exerp_product_privilege
    on #s_exerp_product_privilege_inserts.bk_hash = p_exerp_product_privilege.bk_hash
   and p_exerp_product_privilege.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_exerp_product_privilege
    on p_exerp_product_privilege.bk_hash = s_exerp_product_privilege.bk_hash
   and p_exerp_product_privilege.s_exerp_product_privilege_id = s_exerp_product_privilege.s_exerp_product_privilege_id
 where s_exerp_product_privilege.s_exerp_product_privilege_id is null
    or (s_exerp_product_privilege.s_exerp_product_privilege_id is not null
        and s_exerp_product_privilege.dv_hash <> #s_exerp_product_privilege_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_exerp_product_privilege @current_dv_batch_id

end
