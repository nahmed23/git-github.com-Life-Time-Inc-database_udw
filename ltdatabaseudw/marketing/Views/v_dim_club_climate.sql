CREATE VIEW [marketing].[v_dim_club_climate]
AS select 
     v_dim_club.dim_club_key dim_club_key,
     v_dim_club.club_id club_id,
     CASE 
	     WHEN state IN('CO','IA','IL','IN','KS','MA','MD','MI','MN','MO','NE','NJ','NY','OH','ON','PA','UT','WA','WI') THEN 'Cold' 
		     ELSE 'Warm' END AS climate
FROM [marketing].[v_dim_club];