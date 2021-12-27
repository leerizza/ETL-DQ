
-- Use the `ref` function to select from other models

select *
from {{ ref('dqa_uniqueness') }}
;
select *
from {{ ref('dqa_completeness') }}
;
select *
from {{ ref('dqa_timeliness') }}
;
select *
from {{ ref('dqa_validity') }}
;
select *
from {{ ref('dqa_all_criteria') }}


