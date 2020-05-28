CREATE PROC [dbo].[proc_d_mms_drawer] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_mms_drawer)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_mms_drawer_insert') is not null drop table #p_mms_drawer_insert
create table dbo.#p_mms_drawer_insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_drawer.p_mms_drawer_id,
       p_mms_drawer.bk_hash
  from dbo.p_mms_drawer
 where p_mms_drawer.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_mms_drawer.dv_batch_id > @max_dv_batch_id
        or p_mms_drawer.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_mms_drawer.bk_hash,
       p_mms_drawer.bk_hash dim_mms_drawer_key,
       p_mms_drawer.drawer_id drawer_id,
       l_mms_drawer.club_id club_id,
       isnull(s_mms_drawer.description,'') description,
       case when p_mms_drawer.bk_hash in ('-997','-998','-999') then p_mms_drawer.bk_hash
            when l_mms_drawer.club_id is null then '-998'
            else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_mms_drawer.club_id as varchar(500)),'z#@$k%&P'))),2)
        end dim_club_key,
       case when p_mms_drawer.bk_hash in ('-997','-998','-999') then NULL
            when s_mms_drawer.locked_flag = 1 then 'Y'
            else 'N'
        end locked_flag,
       starting_cash_amount starting_cash_amount,
       p_mms_drawer.p_mms_drawer_id,
       p_mms_drawer.dv_batch_id,
       p_mms_drawer.dv_load_date_time,
       p_mms_drawer.dv_load_end_date_time
  from dbo.h_mms_drawer
  join dbo.p_mms_drawer
    on h_mms_drawer.bk_hash = p_mms_drawer.bk_hash  join #p_mms_drawer_insert
    on p_mms_drawer.bk_hash = #p_mms_drawer_insert.bk_hash
   and p_mms_drawer.p_mms_drawer_id = #p_mms_drawer_insert.p_mms_drawer_id
  join dbo.l_mms_drawer
    on p_mms_drawer.bk_hash = l_mms_drawer.bk_hash
   and p_mms_drawer.l_mms_drawer_id = l_mms_drawer.l_mms_drawer_id
  join dbo.s_mms_drawer
    on p_mms_drawer.bk_hash = s_mms_drawer.bk_hash
   and p_mms_drawer.s_mms_drawer_id = s_mms_drawer.s_mms_drawer_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_mms_drawer
   where d_mms_drawer.bk_hash in (select bk_hash from #p_mms_drawer_insert)

  insert dbo.d_mms_drawer(
             bk_hash,
             dim_mms_drawer_key,
             drawer_id,
             club_id,
             description,
             dim_club_key,
             locked_flag,
             starting_cash_amount,
             p_mms_drawer_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         dim_mms_drawer_key,
         drawer_id,
         club_id,
         description,
         dim_club_key,
         locked_flag,
         starting_cash_amount,
         p_mms_drawer_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_mms_drawer)
--Done!
end
