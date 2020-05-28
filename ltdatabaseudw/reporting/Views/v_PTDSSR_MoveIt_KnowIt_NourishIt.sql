CREATE VIEW [reporting].[v_PTDSSR_MoveIt_KnowIt_NourishIt]
AS SELECT Hierarchy.dim_reporting_hierarchy_key,
       Hierarchy.reporting_division, 
	   Hierarchy.reporting_sub_division,
	   Hierarchy.reporting_department,
	   Hierarchy.reporting_product_group,
       CASE WHEN Hierarchy.reporting_sub_division in('Endurance','Group Training','Mixed Combat Arts')				
	                 OR Hierarchy.reporting_department in('Pilates','Personal Training','Fitness Products','Lutsen 99er')			
			THEN 'Move It'
		    WHEN Hierarchy.reporting_department in('Devices','Lab Testing','Metabolic Assessments','MyHealth Check','MyHealth Score','Metabolic Conditioning')				
	        THEN 'Know It'	
		    ELSE 'Nourish It'
				END PTDSSRCategory,
	   CASE WHEN Hierarchy.reporting_sub_division in('Endurance','Group Training','Mixed Combat Arts')				
	                 OR Hierarchy.reporting_department in('Pilates','Personal Training','Fitness Products','Lutsen 99er')			
			THEN 3
		    WHEN Hierarchy.reporting_department in('Devices','Lab Testing','Metabolic Assessments','MyHealth Check','MyHealth Score','Metabolic Conditioning')				
	        THEN 1	
			ELSE 2
				END CategoryDisplayOrder,
	  CASE WHEN Hierarchy.reporting_department in('Small Group','LTF at Home')
           THEN 'Small Group'
	       WHEN Hierarchy.reporting_department in('LT Endurance','Golf','Cycle-PT','Run-PT','Tri-PT','Lutsen 99er')
	       THEN 'LT Endurance'
	       WHEN Hierarchy.reporting_department in('myHealth Check','myHealth Score')
	       THEN 'myHealth Score'
	       WHEN Hierarchy.reporting_department in('PT E-Commerce','PT Nutritionals')
	       THEN 'Nutritional Products'
	       ELSE Hierarchy.reporting_department
	            END PTDSSRRowLabel,
		CASE WHEN DimDate.calendar_date > getdate()
		     THEN 'Y'
			 ELSE 'N'
			 END ActiveFlag,
        Hierarchy.effective_dim_date_key,
        Hierarchy.expiration_dim_date_key
  
FROM [marketing].[v_dim_reporting_hierarchy_history]  Hierarchy
  JOIN [marketing].[v_dim_date] DimDate
    ON Hierarchy.expiration_dim_date_key = DimDate.dim_date_key;