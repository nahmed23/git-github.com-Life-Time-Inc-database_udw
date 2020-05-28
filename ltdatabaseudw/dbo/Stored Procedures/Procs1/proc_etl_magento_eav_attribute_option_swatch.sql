CREATE PROC [dbo].[proc_etl_magento_eav_attribute_option_swatch] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_magento_eav_attribute_option_swatch

set @insert_date_time = getdate()
insert into dbo.stage_hash_magento_eav_attribute_option_swatch (
       bk_hash,
       swatch_id,
       option_id,
       store_id,
       type,
       value,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(swatch_id as varchar(500)),'z#@$k%&P'))),2) bk_hash,
       swatch_id,
       option_id,
       store_id,
       type,
       value,
       dummy_modified_date_time,
       isnull(cast(stage_magento_eav_attribute_option_swatch.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       dv_batch_id
  from stage_magento_eav_attribute_option_swatch
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_magento_eav_attribute_option_swatch @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_magento_eav_attribute_option_swatch (
       bk_hash,
       swatch_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_magento_eav_attribute_option_swatch.bk_hash,
       stage_hash_magento_eav_attribute_option_swatch.swatch_id swatch_id,
       isnull(cast(stage_hash_magento_eav_attribute_option_swatch.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       39,
       @insert_date_time,
       @user
  from stage_hash_magento_eav_attribute_option_swatch
  left join h_magento_eav_attribute_option_swatch
    on stage_hash_magento_eav_attribute_option_swatch.bk_hash = h_magento_eav_attribute_option_swatch.bk_hash
 where h_magento_eav_attribute_option_swatch_id is null
   and stage_hash_magento_eav_attribute_option_swatch.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current l_magento_eav_attribute_option_swatch
if object_id('tempdb..#l_magento_eav_attribute_option_swatch_inserts') is not null drop table #l_magento_eav_attribute_option_swatch_inserts
create table #l_magento_eav_attribute_option_swatch_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_eav_attribute_option_swatch.bk_hash,
       stage_hash_magento_eav_attribute_option_swatch.swatch_id swatch_id,
       stage_hash_magento_eav_attribute_option_swatch.option_id option_id,
       stage_hash_magento_eav_attribute_option_swatch.store_id store_id,
       isnull(cast(stage_hash_magento_eav_attribute_option_swatch.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute_option_swatch.swatch_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute_option_swatch.option_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute_option_swatch.store_id as varchar(500)),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_eav_attribute_option_swatch
 where stage_hash_magento_eav_attribute_option_swatch.dv_batch_id = @current_dv_batch_id

--Insert all updated and new l_magento_eav_attribute_option_swatch records
set @insert_date_time = getdate()
insert into l_magento_eav_attribute_option_swatch (
       bk_hash,
       swatch_id,
       option_id,
       store_id,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #l_magento_eav_attribute_option_swatch_inserts.bk_hash,
       #l_magento_eav_attribute_option_swatch_inserts.swatch_id,
       #l_magento_eav_attribute_option_swatch_inserts.option_id,
       #l_magento_eav_attribute_option_swatch_inserts.store_id,
       case when l_magento_eav_attribute_option_swatch.l_magento_eav_attribute_option_swatch_id is null then isnull(#l_magento_eav_attribute_option_swatch_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #l_magento_eav_attribute_option_swatch_inserts.source_hash,
       @insert_date_time,
       @user
  from #l_magento_eav_attribute_option_swatch_inserts
  left join p_magento_eav_attribute_option_swatch
    on #l_magento_eav_attribute_option_swatch_inserts.bk_hash = p_magento_eav_attribute_option_swatch.bk_hash
   and p_magento_eav_attribute_option_swatch.dv_load_end_date_time = 'Dec 31, 9999'
  left join l_magento_eav_attribute_option_swatch
    on p_magento_eav_attribute_option_swatch.bk_hash = l_magento_eav_attribute_option_swatch.bk_hash
   and p_magento_eav_attribute_option_swatch.l_magento_eav_attribute_option_swatch_id = l_magento_eav_attribute_option_swatch.l_magento_eav_attribute_option_swatch_id
 where l_magento_eav_attribute_option_swatch.l_magento_eav_attribute_option_swatch_id is null
    or (l_magento_eav_attribute_option_swatch.l_magento_eav_attribute_option_swatch_id is not null
        and l_magento_eav_attribute_option_swatch.dv_hash <> #l_magento_eav_attribute_option_swatch_inserts.source_hash)

--calculate hash and lookup to current s_magento_eav_attribute_option_swatch
if object_id('tempdb..#s_magento_eav_attribute_option_swatch_inserts') is not null drop table #s_magento_eav_attribute_option_swatch_inserts
create table #s_magento_eav_attribute_option_swatch_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_magento_eav_attribute_option_swatch.bk_hash,
       stage_hash_magento_eav_attribute_option_swatch.swatch_id swatch_id,
       stage_hash_magento_eav_attribute_option_swatch.type type,
       stage_hash_magento_eav_attribute_option_swatch.value value,
       stage_hash_magento_eav_attribute_option_swatch.dummy_modified_date_time dummy_modified_date_time,
       isnull(cast(stage_hash_magento_eav_attribute_option_swatch.dummy_modified_date_time as datetime),'Jan 1, 1753') dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute_option_swatch.swatch_id as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(cast(stage_hash_magento_eav_attribute_option_swatch.type as varchar(500)),'z#@$k%&P')
                                        +'P%#&z$@k'+isnull(stage_hash_magento_eav_attribute_option_swatch.value,'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_magento_eav_attribute_option_swatch
 where stage_hash_magento_eav_attribute_option_swatch.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_magento_eav_attribute_option_swatch records
set @insert_date_time = getdate()
insert into s_magento_eav_attribute_option_swatch (
       bk_hash,
       swatch_id,
       type,
       value,
       dummy_modified_date_time,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_magento_eav_attribute_option_swatch_inserts.bk_hash,
       #s_magento_eav_attribute_option_swatch_inserts.swatch_id,
       #s_magento_eav_attribute_option_swatch_inserts.type,
       #s_magento_eav_attribute_option_swatch_inserts.value,
       #s_magento_eav_attribute_option_swatch_inserts.dummy_modified_date_time,
       case when s_magento_eav_attribute_option_swatch.s_magento_eav_attribute_option_swatch_id is null then isnull(#s_magento_eav_attribute_option_swatch_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       39,
       #s_magento_eav_attribute_option_swatch_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_magento_eav_attribute_option_swatch_inserts
  left join p_magento_eav_attribute_option_swatch
    on #s_magento_eav_attribute_option_swatch_inserts.bk_hash = p_magento_eav_attribute_option_swatch.bk_hash
   and p_magento_eav_attribute_option_swatch.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_magento_eav_attribute_option_swatch
    on p_magento_eav_attribute_option_swatch.bk_hash = s_magento_eav_attribute_option_swatch.bk_hash
   and p_magento_eav_attribute_option_swatch.s_magento_eav_attribute_option_swatch_id = s_magento_eav_attribute_option_swatch.s_magento_eav_attribute_option_swatch_id
 where s_magento_eav_attribute_option_swatch.s_magento_eav_attribute_option_swatch_id is null
    or (s_magento_eav_attribute_option_swatch.s_magento_eav_attribute_option_swatch_id is not null
        and s_magento_eav_attribute_option_swatch.dv_hash <> #s_magento_eav_attribute_option_swatch_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_magento_eav_attribute_option_swatch @current_dv_batch_id

end
