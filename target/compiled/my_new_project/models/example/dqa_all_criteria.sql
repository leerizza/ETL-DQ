

select * from `playground-325606.dataset_dbt.dqa_uniqueness`
union all
select * from `playground-325606.dataset_dbt.dqa_completeness`
union all
select * from `playground-325606.dataset_dbt.dqa_validity`
union all
select * from `playground-325606.dataset_dbt.dqa_timeliness`