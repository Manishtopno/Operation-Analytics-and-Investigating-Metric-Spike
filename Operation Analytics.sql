create database Operation_Analytics;
use Operation_Analytics;

create table users(
user_id int,
created_at Varchar(100),
company_id int,
language varchar(50),
activated_at Varchar(100),
state varchar(50));

show variables like 'secure_file_priv';

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

alter table users add column temp_created_at datetime;

set SQL_safe_updates= 0;

update users set temp_created_at= str_to_date(created_at, '%d-%m-%Y %H:%i' );

set SQL_safe_updates=1;

alter table users drop column created_at;

alter table users change column temp_created_at  created_at datetime;

select * from users;
 
create table events(
user_id int,
occurred_at varchar(100),
event_type varchar(50),
event_name varchar(100),
location varchar(50),
device varchar(50),
user_type int);

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

desc events;

select * from events;

alter table events add column temp_occurred_at datetime;

set SQL_safe_updates= 0;

update events set temp_occurred_at= str_to_date(occurred_at, '%d-%m-%Y %H:%i' );

set SQL_safe_updates=1;

alter table events drop column occurred_at;

alter table events change column temp_occurred_at  occurred_at datetime;

Create table email_events(
user_id int,
occurred_at varchar(100),
action varchar(100),
user_type int);

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table email_events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from email_events;

alter table email_events add column  temp_occurred_at datetime;

set SQL_safe_updates= 0;

update email_events set temp_occurred_at= str_to_date(occurred_at, '%d-%m-%Y  %H:%i');

set SQL_safe_updates= 1;

alter table email_events drop column occurred_at;

alter table email_events change column temp_occurred_at occurred_at datetime;

CREATE TABLE job_data (
    ds DATE,
    job_id INT,
    actor_id INT,
    event VARCHAR(10),
    language VARCHAR(10),
    time_spent INT,
    org CHAR(2)
    );

insert into job_data(ds,job_id,actor_id,event,language,time_spent,org)
values('2020-11-30','21','1001','skip','English','15','A'),
('2020-11-30','22','1006','transfer','Arabic','25','B'),
('2020-11-29','23','1003','decision','Persian','20','C'),
('2020-11-28','23','1005','transfer','Persian','22','D'),
('2020-11-28','25','1002','decision','Hindi','11','B'),
('2020-11-27','11','1007','decision','French','104','D'),
('2020-11-26','23','1004','skip','Persian','56','A'),
('2020-11-25','20','1003','transfer','Italian','45','C');

select * from job_data;

# Case study 1
# Write an SQL query to calculate the number of jobs reviewed per hour for each day in november 2020.
SELECT 
    ds AS DATE,
    COUNT(job_id),
    ROUND((SUM(time_spent) / 3600), 2) AS Total_time_spent_per_hour,
    ROUND((COUNT(job_id) / (SUM(time_spent) / 3600)),
            2) AS Job_review_per_day
FROM
    job_data
WHERE
    ds BETWEEN '2020-11-01' AND '2020-11-30' GROUP BY ds ORDER BY ds;
    
# Write an SQL query to calculate the 7-day rolling average of throughput. 
# Additionally, explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why.
select round(count(event)/sum(time_spent),2) as 'Daily throughput' from job_data
group by ds order by ds;

# Write an SQL query to calculate the percentage share of each language over the last 30 days.
select language, round(100*count(*)/total,2) as percentage, sub.total from job_data 
cross join(select count(*) as total from job_data) as sub group by language, sub.total;

# Write an SQL query to display duplicate rows from the job_data table.
select actor_id, count(*) as duplicates from job_data
group by   actor_id having count(*)>1;

# Case Study 2
# Write an SQL query to calculate the weekly user engagement.
 
 SELECT 
    EXTRACT(WEEK FROM occurred_at) AS week_num,
    COUNT(DISTINCT user_id) AS active_users
FROM
    events
WHERE
    event_type = 'engagement'
GROUP BY week_num
ORDER BY week_num;

#  Write an SQL query to calculate the user growth for the product
 with Growth_of_users as(
 SELECT extract(YEAR from created_at) as YEAR,
extract(WEEk from created_at) as week_number,
count(distinct user_id) as active_users 
from users
group by YEAR, week_number)
select YEAR, week_number,active_users,
sum(active_users) over (order by YEAR, week_number) as cumulative_users 
from Growth_of_users
order by YEAR, week_number;

# Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.

select extract(week from occurred_at) as weeks,
count(distinct user_id) as no_of_users 
from events
where event_type="singnup_flow" or  event_name="complete_signup"
group by weeks order by weeks ;

#  calculate the weekly engagement per device.

select device, extract(week from occurred_at) as weeks,
count(distinct user_id) as no_of_users from events
where event_type="engagement"
group by device,weeks order by weeks;

#  calculate the email engagement metrics.

select action, count(distinct user_id) as engaged_users
from email_events group by action;



    






