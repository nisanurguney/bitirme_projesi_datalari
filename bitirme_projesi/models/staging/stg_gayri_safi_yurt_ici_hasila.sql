select *
from {{ source('saglik','gayri_safi_yurt_ici_hasila_clean_data') }}