select
    year,
    physician_count,
    visits_per_physician,
    total_policies,

    round(total_policies / physician_count,2) as policies_per_physician,

    round(visits_per_physician /
    (total_policies / physician_count),2) as visit_policy_ratio

from {{ ref('int_talep_dinamigi') }}
