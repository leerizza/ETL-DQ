

  create or replace view `playground-325606`.`dataset_dbt`.`view_models_dqa`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `playground-325606`.`dataset_dbt`.`dqa_uniqueness`
;
select *
from `playground-325606`.`dataset_dbt`.`dqa_completeness`
;
select *
from `playground-325606`.`dataset_dbt`.`dqa_timeliness`
;
select *
from `playground-325606`.`dataset_dbt`.`dqa_validity`
;
select *
from `playground-325606`.`dataset_dbt`.`dqa_all_criteria`;

