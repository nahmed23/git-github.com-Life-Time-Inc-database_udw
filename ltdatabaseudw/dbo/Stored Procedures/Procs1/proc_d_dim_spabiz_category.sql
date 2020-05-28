﻿CREATE PROC [dbo].[proc_d_dim_spabiz_category] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_category','proc_d_dim_spabiz_category start',@current_dv_batch_id

-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_category','max dv_batch_id',@current_dv_batch_id
if object_id('tempdb..#batch_id') is not null drop table #batch_id
create table dbo.#batch_id with(distribution=round_robin, location=user_db, heap) as
select isnull(max(dv_batch_id),-2) max_dv_batch_id,
       @current_dv_batch_id as current_dv_batch_id
  from dbo.d_dim_spabiz_category

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_category','#p_spabiz_category_insert',@current_dv_batch_id
if object_id('tempdb..#p_spabiz_category_insert') is not null drop table #p_spabiz_category_insert
create table dbo.#p_spabiz_category_insert with(distribution=round_robin, location=user_db, heap) as
select p_spabiz_category.p_spabiz_category_id,
       p_spabiz_category.bk_hash,
       row_number() over (order by p_spabiz_category_id) row_num
  from dbo.p_spabiz_category
  join #batch_id
    on p_spabiz_category.dv_batch_id > #batch_id.max_dv_batch_id
    or p_spabiz_category.dv_batch_id = #batch_id.current_dv_batch_id
 where p_spabiz_category.dv_load_end_date_time = convert(datetime,'9999.12.31',102)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_category','#insert',@current_dv_batch_id
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=round_robin, location=user_db, heap) as
select #p_spabiz_category_insert.row_num,
       p_spabiz_category.bk_hash dim_spabiz_category_key,
       p_spabiz_category.category_id category_id,
       p_spabiz_category.store_number store_number,
       case when p_spabiz_category.bk_hash in ('-997','-998','-999') then ''
            when s_spabiz_category.name is null then ''
            else s_spabiz_category.name
        end category_name,
       case when p_spabiz_category.bk_hash in ('-997','-998','-999') then null
            when s_spabiz_category.delete_date = convert(date, '18991230', 112) then null
            else delete_date
        end deleted_date_time,
       case when p_spabiz_category.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_category.deleted = -1 then 'Y'
            else 'N'
        end deleted_flag,
       case when p_spabiz_category.bk_hash in ('-997','-998','-999') then p_spabiz_category.bk_hash
            when l_spabiz_category.data_type is null then '-998'
            when l_spabiz_category.data_type = 0 then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_category.data_type as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_category.store_number as varchar(500)),'z#@$k%&P'))),2)
        end dim_spabiz_data_type_key,
       case when p_spabiz_category.bk_hash in ('-997','-998','-999') then p_spabiz_category.bk_hash       
            when l_spabiz_category.store_id is null then '-998'     
       	 when l_spabiz_category.store_id = 0 then '-998'       
       	 else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_category.store_number as varchar(500)),'z#@$k%&P'))),2)   
       end dim_spabiz_store_key,
       s_spabiz_category.edit_time edit_date_time,
       convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_spabiz_category.parent_id as varchar(500)),'z#@$k%&P')+'P%#&z$@k'+isnull(cast(l_spabiz_category.store_number as varchar(500)),'z#@$k%&P'))),2) parent_category_bk_hash,
       case when p_spabiz_category.bk_hash in ('-997','-998','-999') then 'N'
            when s_spabiz_category.level = 2 then 'Y'
            else 'N'
        end sub_category_flag,
       l_spabiz_category.gl_account l_spabiz_category_gl_account,
       l_spabiz_category.parent_id l_spabiz_category_parent_id,
       p_spabiz_category.p_spabiz_category_id,
       p_spabiz_category.dv_batch_id,
       p_spabiz_category.dv_load_date_time,
       p_spabiz_category.dv_load_end_date_time
  from dbo.p_spabiz_category
  join #p_spabiz_category_insert
    on p_spabiz_category.p_spabiz_category_id = #p_spabiz_category_insert.p_spabiz_category_id
  join dbo.l_spabiz_category
    on p_spabiz_category.l_spabiz_category_id = l_spabiz_category.l_spabiz_category_id
  join dbo.s_spabiz_category
    on p_spabiz_category.s_spabiz_category_id = s_spabiz_category.s_spabiz_category_id
 where l_spabiz_category.store_number not in (1,100,999) OR p_spabiz_category.bk_hash in ('-999','-998','-997')

declare @start int, @end int, @task_description varchar(50)
declare @start_p_id bigint
declare @insert_count bigint
set @start = 1
set @end = (select max(row_num) from #insert)

while @start <= @end
begin

    set @insert_count = isnull((select count(*) from #insert where row_num >= @start and row_num < @start+1000000),0)
    exec dbo.proc_util_sequence_number_get_next @table_name = 'd_dim_spabiz_category', @id_count = @insert_count, @start_id = @start_p_id out

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
    set @task_description = 'final insert/update '+cast(@start as varchar)+' of '+cast(@end as varchar)
    exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_category',@task_description,@current_dv_batch_id
    begin tran
      delete dbo.d_dim_spabiz_category
       where d_dim_spabiz_category.dim_spabiz_category_key in (select bk_hash from #p_spabiz_category_insert where row_num >= @start and row_num < @start+1000000)

      insert dbo.d_dim_spabiz_category(
                 d_dim_spabiz_category_id,
                 dim_spabiz_category_key,
                 category_id,
                 store_number,
                 category_name,
                 deleted_date_time,
                 deleted_flag,
                 dim_spabiz_data_type_key,
                 dim_spabiz_store_key,
                 edit_date_time,
                 parent_category_bk_hash,
                 sub_category_flag,
                 l_spabiz_category_gl_account,
                 l_spabiz_category_parent_id,
                 p_spabiz_category_id,
                 dv_load_date_time,
                 dv_load_end_date_time,
                 dv_batch_id,
                 dv_inserted_date_time,
                 dv_insert_user)
      select @start_p_id + row_num,
             dim_spabiz_category_key,
             category_id,
             store_number,
             category_name,
             deleted_date_time,
             deleted_flag,
             dim_spabiz_data_type_key,
             dim_spabiz_store_key,
             edit_date_time,
             parent_category_bk_hash,
             sub_category_flag,
             l_spabiz_category_gl_account,
             l_spabiz_category_parent_id,
             p_spabiz_category_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             getdate(),
             suser_sname()
        from #insert
       where row_num >= @start
         and row_num < @start+1000000
    commit tran

    set @start = @start+1000000
end

--Done!
exec dbo.proc_util_task_status_insert 'proc_d_dim_spabiz_category','proc_d_dim_spabiz_category end',@current_dv_batch_id
end
