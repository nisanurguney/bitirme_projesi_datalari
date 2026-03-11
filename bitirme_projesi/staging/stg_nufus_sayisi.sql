select *
from {{ source('saglik','nufus_sayisi_clean_data') }}