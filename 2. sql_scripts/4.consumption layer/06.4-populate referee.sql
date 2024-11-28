select info:officials from cricket.raw.match_raw_tbl;

-- referee_id int primary key autoincrement,
-- referee_name text not null,
-- referee_type text not null



select 
   case 
   when ref.key='match_referees' then 'match_referee'
   when ref.key='reserve_umpires' then 'reserve_umpire'
   when ref.key='tv_umpires' then 'tv_umpire'
   when ref.key='umpires' then
     case 
        when ref_name.index=0 then 'first_umpire'
        when ref_name.index=1 then 'second_umpire'
     else 'NA'
     end
    else 'NA'
    end as referee_type,
    ref_name.value as referee_name
from cricket.raw.match_raw_tbl raw,
lateral flatten(input => raw.info:officials) ref,
lateral flatten(input => ref.value) ref_name,
group by referee_type, referee_name;

insert into cricket.consumption.referee_dim(referee_name , referee_type)
select 
   case 
   when ref.key='match_referees' then 'match_referee'
   when ref.key='reserve_umpires' then 'reserve_umpire'
   when ref.key='tv_umpires' then 'tv_umpire'
   when ref.key='umpires' then
     case 
        when ref_name.index=0 then 'first_umpire'
        when ref_name.index=1 then 'second_umpire'
     else 'NA'
     end
    else 'NA'
    end as referee_type,
    ref_name.value as referee_name
from cricket.raw.match_raw_tbl raw,
lateral flatten(input => raw.info:officials) ref,
lateral flatten(input => ref.value) ref_name,
group by referee_type, referee_name;