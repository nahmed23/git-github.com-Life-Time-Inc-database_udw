CREATE PROC [dbo].[proc_d_exerp_participation] @current_dv_batch_id [bigint] AS
begin

set nocount on
set xact_abort on

--Start!
-- Get the @max_dv_batch_id from dimension/fact.  Use -2 if there aren't any records.
declare @max_dv_batch_id bigint = (select isnull(max(dv_batch_id),-2) from d_exerp_participation)

-- Find the PIT records with dv_batch_id > @max_dv_batch_id
-- and find the PIT records with dv_batch_id = @current_dv_batch_id since those need to be deleted in case of a retry
if object_id('tempdb..#p_exerp_participation_insert') is not null drop table #p_exerp_participation_insert
create table dbo.#p_exerp_participation_insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_participation.p_exerp_participation_id,
       p_exerp_participation.bk_hash
  from dbo.p_exerp_participation
 where p_exerp_participation.dv_load_end_date_time = convert(datetime,'9999.12.31',102)
   and (p_exerp_participation.dv_batch_id > @max_dv_batch_id
        or p_exerp_participation.dv_batch_id = @current_dv_batch_id)

-- calculate all values of the records to be inserted to make the actual update go as fast as possible
if object_id('tempdb..#insert') is not null drop table #insert
create table dbo.#insert with(distribution=hash(bk_hash), location=user_db) as
select p_exerp_participation.bk_hash,
       p_exerp_participation.bk_hash fact_exerp_participation_key ,
       p_exerp_participation.participation_id participation_id,
       case when p_exerp_participation.bk_hash in ('-997','-998','-999') then p_exerp_participation.bk_hash
           when s_exerp_participation.cancel_datetime is null then '-998'
       else convert(varchar, s_exerp_participation.cancel_datetime, 112) end cancel_dim_date_key,
       case when p_exerp_participation.bk_hash in ('-997','-998','-999') then p_exerp_participation.bk_hash
           when s_exerp_participation.cancel_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_participation.cancel_datetime,114), 1, 5),':','') end cancel_dim_time_key,
       s_exerp_participation.cancel_interface_type cancel_interface_type,
       s_exerp_participation.cancel_reason cancel_reason,
       case when p_exerp_participation.bk_hash in ('-997','-998','-999') then p_exerp_participation.bk_hash
           when s_exerp_participation.creation_datetime is null then '-998'
       else convert(varchar, s_exerp_participation.creation_datetime, 112) end creation_dim_date_key,
       case when p_exerp_participation.bk_hash in ('-997','-998','-999') then p_exerp_participation.bk_hash
           when s_exerp_participation.creation_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_participation.creation_datetime,114), 1, 5),':','') end creation_dim_time_key,
       case when p_exerp_participation.bk_hash in('-997', '-998', '-999') then p_exerp_participation.bk_hash
           when l_exerp_participation.center_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_participation.center_id as int) as varchar(500)),'z#@$k%&P'))),2)    end dim_club_key,
       case when p_exerp_participation.bk_hash in('-997', '-998', '-999') then p_exerp_participation.bk_hash
           when l_exerp_participation.booking_id is null then '-998'
        else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(l_exerp_participation.booking_id as varchar(4000)),'z#@$k%&P'))),2) end dim_exerp_booking_key,
       case when p_exerp_participation.bk_hash in('-997', '-998', '-999') then p_exerp_participation.bk_hash 
              when ((l_exerp_participation.person_id is null) OR (l_exerp_participation.person_id LIKE '%e%') or (l_exerp_participation.person_id LIKE '%OLDe%')
       	    or (len(l_exerp_participation.person_id) > 9)  or (d_exerp_person.person_type = 'STAFF' and l_exerp_participation.person_id not LIKE '%e%') 
       		  or (d_exerp_person.person_type = 'STAFF')    or (isnumeric(l_exerp_participation.person_id) = 0)) then '-998' 
       		       else convert(char(32),hashbytes('md5',('P%#&z$@k'+isnull(cast(cast(l_exerp_participation.person_id as int) as varchar(500)),'z#@$k%&P'))),2) end dim_mms_member_key,
       s_exerp_participation.ets ets,
       s_exerp_participation_1.participant_number participant_number,
       s_exerp_participation.state participation_state,
       s_exerp_participation_1.seat_id seat_id,
       s_exerp_participation_1.seat_obtained_datetime seat_obtained_datetime,
       case when p_exerp_participation.bk_hash in('-997', '-998', '-999') then p_exerp_participation.bk_hash
           when s_exerp_participation_1.seat_obtained_datetime is null then '-998'
        else convert(varchar, s_exerp_participation_1.seat_obtained_datetime, 112)    end seat_obtained_dim_date_key,
       case when p_exerp_participation.bk_hash in ('-997','-998','-999') then p_exerp_participation.bk_hash
       when s_exerp_participation_1.seat_obtained_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_participation_1.seat_obtained_datetime,114), 1, 5),':','') end seat_obtained_dim_time_key,
       s_exerp_participation_1.seat_state seat_state,
       case when p_exerp_participation.bk_hash in ('-997','-998','-999') then p_exerp_participation.bk_hash
           when s_exerp_participation.show_up_datetime is null then '-998'
       else convert(varchar, s_exerp_participation.show_up_datetime, 112) end show_up_dim_date_key,
       case when p_exerp_participation.bk_hash in ('-997','-998','-999') then p_exerp_participation.bk_hash
           when s_exerp_participation.show_up_datetime is null then '-998'
       else '1' + replace(substring(convert(varchar,s_exerp_participation.show_up_datetime,114), 1, 5),':','') end show_up_dim_time_key,
       s_exerp_participation.show_up_interface_type show_up_interface_type,
       case when s_exerp_participation.show_up_using_card = 1 then 'Y'
             else 'N' end show_up_using_card_flag,
       s_exerp_participation.user_interface_type user_interface_type,
       case when s_exerp_participation.was_on_waiting_list = 1 then 'Y'
             else 'N' end was_on_waiting_list_flag,
       isnull(h_exerp_participation.dv_deleted,0) dv_deleted,
       p_exerp_participation.p_exerp_participation_id,
       p_exerp_participation.dv_batch_id,
       p_exerp_participation.dv_load_date_time,
       p_exerp_participation.dv_load_end_date_time
  from dbo.h_exerp_participation
  join dbo.p_exerp_participation
    on h_exerp_participation.bk_hash = p_exerp_participation.bk_hash
  join #p_exerp_participation_insert
    on p_exerp_participation.bk_hash = #p_exerp_participation_insert.bk_hash
   and p_exerp_participation.p_exerp_participation_id = #p_exerp_participation_insert.p_exerp_participation_id
  join dbo.l_exerp_participation
    on p_exerp_participation.bk_hash = l_exerp_participation.bk_hash
   and p_exerp_participation.l_exerp_participation_id = l_exerp_participation.l_exerp_participation_id
  join dbo.s_exerp_participation
    on p_exerp_participation.bk_hash = s_exerp_participation.bk_hash
   and p_exerp_participation.s_exerp_participation_id = s_exerp_participation.s_exerp_participation_id
  join dbo.s_exerp_participation_1
    on p_exerp_participation.bk_hash = s_exerp_participation_1.bk_hash
   and p_exerp_participation.s_exerp_participation_1_id = s_exerp_participation_1.s_exerp_participation_1_id
 left join 	d_exerp_person		on l_exerp_participation.person_id = d_exerp_person.person_id

-- do as a single transaction
--   delete records from the dimensional table where the business_key is in #p_*_insert temp table
--   insert records from all of the joins to the pit table and to #p_*_insert temp table
begin tran
  delete dbo.d_exerp_participation
   where d_exerp_participation.bk_hash in (select bk_hash from #p_exerp_participation_insert)

  insert dbo.d_exerp_participation(
             bk_hash,
             fact_exerp_participation_key ,
             participation_id,
             cancel_dim_date_key,
             cancel_dim_time_key,
             cancel_interface_type,
             cancel_reason,
             creation_dim_date_key,
             creation_dim_time_key,
             dim_club_key,
             dim_exerp_booking_key,
             dim_mms_member_key,
             ets,
             participant_number,
             participation_state,
             seat_id,
             seat_obtained_datetime,
             seat_obtained_dim_date_key,
             seat_obtained_dim_time_key,
             seat_state,
             show_up_dim_date_key,
             show_up_dim_time_key,
             show_up_interface_type,
             show_up_using_card_flag,
             user_interface_type,
             was_on_waiting_list_flag,
             deleted_flag,
             p_exerp_participation_id,
             dv_load_date_time,
             dv_load_end_date_time,
             dv_batch_id,
             dv_inserted_date_time,
             dv_insert_user)
  select bk_hash,
         fact_exerp_participation_key ,
         participation_id,
         cancel_dim_date_key,
         cancel_dim_time_key,
         cancel_interface_type,
         cancel_reason,
         creation_dim_date_key,
         creation_dim_time_key,
         dim_club_key,
         dim_exerp_booking_key,
         dim_mms_member_key,
         ets,
         participant_number,
         participation_state,
         seat_id,
         seat_obtained_datetime,
         seat_obtained_dim_date_key,
         seat_obtained_dim_time_key,
         seat_state,
         show_up_dim_date_key,
         show_up_dim_time_key,
         show_up_interface_type,
         show_up_using_card_flag,
         user_interface_type,
         was_on_waiting_list_flag,
         dv_deleted,
         p_exerp_participation_id,
         dv_load_date_time,
         dv_load_end_date_time,
         dv_batch_id,
         getdate(),
         suser_sname()
    from #insert
commit tran

--force replication
set @max_dv_batch_id = (select isnull(max(dv_batch_id),-2) from d_exerp_participation)
--Done!
end
