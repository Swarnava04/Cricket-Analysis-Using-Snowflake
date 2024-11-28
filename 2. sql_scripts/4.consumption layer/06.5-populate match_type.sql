select match_type from cricket.clean.match_detail_clean group by match_type;

insert into cricket.consumption.match_type_dim(match_type)
select 
    case
        when match_type is null then 'NA'
        else match_type
    end as match_type
 from cricket.clean.match_detail_clean
group by match_type;