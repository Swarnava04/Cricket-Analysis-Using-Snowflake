insert into cricket.consumption.venue_dim(venue_name, city)
select venue, city from
(select 
    venue,
    case when city is null then 'NA'
    else city
    end as city
from cricket.clean.match_detail_clean
)
group by 
    venue, city
;

