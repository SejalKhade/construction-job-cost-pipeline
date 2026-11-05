select
    d::date                         as date_day,
    year(d)                         as year,
    month(d)                        as month,
    strftime(d, '%Y-%m')            as year_month,
    weekofyear(d)                   as week_of_year,
    dayname(d)                      as day_name,
    dayofweek(d) in (0, 6)          as is_weekend
from range(date '2026-01-01', date '2027-12-31', interval 1 day) t(d)
