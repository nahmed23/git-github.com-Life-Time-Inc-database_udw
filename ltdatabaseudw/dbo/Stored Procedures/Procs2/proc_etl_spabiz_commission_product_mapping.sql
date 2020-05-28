CREATE PROC [dbo].[proc_etl_spabiz_commission_product_mapping] @current_dv_batch_id [bigint],@job_start_date_time_varchar [varchar](19) AS
begin

set nocount on
set xact_abort on

--Start!
declare @job_start_date_time datetime
set @job_start_date_time = convert(datetime,@job_start_date_time_varchar,120)

declare @user varchar(50) = suser_sname()
declare @insert_date_time datetime

truncate table stage_hash_spabiz_CommissionProductMapping

set @insert_date_time = getdate()
insert into dbo.stage_hash_spabiz_CommissionProductMapping (
       bk_hash,
       ProductName,
       MappingGroupName,
       ProductMappingType,
       jan_one,
       dv_load_date_time,
       dv_inserted_date_time,
       dv_insert_user,
       dv_batch_id)
select convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(ProductName,'z#@$k%&P'))),2) bk_hash,
       ProductName,
       MappingGroupName,
       ProductMappingType,
       jan_one,
       isnull(cast(stage_spabiz_CommissionProductMapping.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @insert_date_time,
       @user,
       dv_batch_id
  from stage_spabiz_CommissionProductMapping
 where dv_batch_id = @current_dv_batch_id

--Run PIT proc for retry logic
exec dbo.proc_p_spabiz_commission_product_mapping @current_dv_batch_id

--Insert/update new hub business keys
set @insert_date_time = getdate()
insert into h_spabiz_commission_product_mapping (
       bk_hash,
       product_name,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_inserted_date_time,
       dv_insert_user)
select stage_hash_spabiz_CommissionProductMapping.bk_hash,
       stage_hash_spabiz_CommissionProductMapping.ProductName product_name,
       isnull(cast(stage_hash_spabiz_CommissionProductMapping.jan_one as datetime),'Jan 1, 1753') dv_load_date_time,
       @current_dv_batch_id,
       10,
       @insert_date_time,
       @user
  from stage_hash_spabiz_CommissionProductMapping
  left join h_spabiz_commission_product_mapping
    on stage_hash_spabiz_CommissionProductMapping.bk_hash = h_spabiz_commission_product_mapping.bk_hash
 where h_spabiz_commission_product_mapping_id is null
   and stage_hash_spabiz_CommissionProductMapping.dv_batch_id = @current_dv_batch_id

--calculate hash and lookup to current s_spabiz_commission_product_mapping
if object_id('tempdb..#s_spabiz_commission_product_mapping_inserts') is not null drop table #s_spabiz_commission_product_mapping_inserts
create table #s_spabiz_commission_product_mapping_inserts with (distribution=hash(bk_hash), location=user_db, clustered index (bk_hash)) as 
select stage_hash_spabiz_CommissionProductMapping.bk_hash,
       stage_hash_spabiz_CommissionProductMapping.ProductName product_name,
       stage_hash_spabiz_CommissionProductMapping.MappingGroupName mapping_group_name,
       stage_hash_spabiz_CommissionProductMapping.ProductMappingType product_mapping_type,
       stage_hash_spabiz_CommissionProductMapping.jan_one jan_one,
       stage_hash_spabiz_CommissionProductMapping.jan_one dv_load_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(stage_hash_spabiz_CommissionProductMapping.ProductName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CommissionProductMapping.MappingGroupName,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(stage_hash_spabiz_CommissionProductMapping.ProductMappingType,'z#@$k%&P')+
                                         'P%#&z$@k'+isnull(convert(varchar,stage_hash_spabiz_CommissionProductMapping.jan_one,120),'z#@$k%&P'))),2) source_hash
  from dbo.stage_hash_spabiz_CommissionProductMapping
 where stage_hash_spabiz_CommissionProductMapping.dv_batch_id = @current_dv_batch_id

--Insert all updated and new s_spabiz_commission_product_mapping records
set @insert_date_time = getdate()
insert into s_spabiz_commission_product_mapping (
       bk_hash,
       product_name,
       mapping_group_name,
       product_mapping_type,
       jan_one,
       dv_load_date_time,
       dv_batch_id,
       dv_r_load_source_id,
       dv_hash,
       dv_inserted_date_time,
       dv_insert_user)
select #s_spabiz_commission_product_mapping_inserts.bk_hash,
       #s_spabiz_commission_product_mapping_inserts.product_name,
       #s_spabiz_commission_product_mapping_inserts.mapping_group_name,
       #s_spabiz_commission_product_mapping_inserts.product_mapping_type,
       #s_spabiz_commission_product_mapping_inserts.jan_one,
       case when s_spabiz_commission_product_mapping.s_spabiz_commission_product_mapping_id is null then isnull(#s_spabiz_commission_product_mapping_inserts.dv_load_date_time,convert(datetime,'jan 1, 1753',120))
            else @job_start_date_time end,
       @current_dv_batch_id,
       10,
       #s_spabiz_commission_product_mapping_inserts.source_hash,
       @insert_date_time,
       @user
  from #s_spabiz_commission_product_mapping_inserts
  left join p_spabiz_commission_product_mapping
    on #s_spabiz_commission_product_mapping_inserts.bk_hash = p_spabiz_commission_product_mapping.bk_hash
   and p_spabiz_commission_product_mapping.dv_load_end_date_time = 'Dec 31, 9999'
  left join s_spabiz_commission_product_mapping
    on p_spabiz_commission_product_mapping.bk_hash = s_spabiz_commission_product_mapping.bk_hash
   and p_spabiz_commission_product_mapping.s_spabiz_commission_product_mapping_id = s_spabiz_commission_product_mapping.s_spabiz_commission_product_mapping_id
 where s_spabiz_commission_product_mapping.s_spabiz_commission_product_mapping_id is null
    or (s_spabiz_commission_product_mapping.s_spabiz_commission_product_mapping_id is not null
        and s_spabiz_commission_product_mapping.dv_hash <> #s_spabiz_commission_product_mapping_inserts.source_hash)

--Run the PIT proc
exec dbo.proc_p_spabiz_commission_product_mapping @current_dv_batch_id

--run dimensional procs
exec dbo.proc_d_spabiz_commission_product_mapping @current_dv_batch_id

end
