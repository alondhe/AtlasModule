
delete from @targetDatabaseSchema.@targetTable
where cohort_definition_id in (@cohortIds);

insert into @targetDatabaseSchema.@targetTable
select * 
from @resultsDatabaseSchema.cohort
where cohort_definition_id in (@cohortIds)
;