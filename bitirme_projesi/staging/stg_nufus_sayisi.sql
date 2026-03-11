select
    city,
    year,
    population
from {{ source('saglik','nufus_sayisi_clean_data') }}