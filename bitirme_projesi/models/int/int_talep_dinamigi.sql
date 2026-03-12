select
    h.date as year,
    h.physician_count,
    h.visits_per_physician,

    s.health,
    s.illness,
    (s.health + s.illness) as total_policies

from {{ ref('stg_hekim_sayilari') }} h

join {{ ref('stg_saglik_sigortasi_police_sayisi') }} s
on h.date = s.date