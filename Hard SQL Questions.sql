--write a SQL Query to derive ICC Points table---

	with total_team as
	(
	select team_1 as teams, count(*) as total_count from icc_world_cup
	group by team_1
	union all
	select team_2 as teams, count(*) as total_count  from icc_world_cup
	group by team_2
	), winner_team as
	(
	select winner,count(*) as wins from icc_world_cup
	group by  winner
	)
	select tm.teams, sum(total_count) as total_matches_played, coalesce(wt.wins,0) as wins,
	(sum(total_count)-coalesce(wt.wins,0)) as losses
	from total_team tm left join winner_team wt
	on tm.teams=wt.winner
	group by teams,  wt.wins
	order by wins desc

-- write a SQL Query to identify No. of New and repeated customer


	with CTE AS
	(select order_date --customer_id-, , order_amount, ROW_NUMBER() over(partition by customer_id order by order_date) as rn,
	,(case when ROW_NUMBER() over(partition by customer_id order by order_date)=1 then 1 ELSE 0 end) as first_order
	,(case when ROW_NUMBER() over(partition by customer_id  order by order_date)>1 then 1 ELSE 0 end) as  Repeat_order
	from customer_orders
	group by order_date, customer_id--, order_amount,
	) select ORDER_DATE, SUM(FIRST_ORDER) as no_of_new_customer,SUM(Repeat_order) as no_of_repeat_customer from CTE
	group by order_date

--write a SQL Query to identify no. of visit and resource used by customer

	with total_floor as
	(select name, count(*) as total_floor
	from entries
	group by name
	), high_visit as
	(select  name, floor, count(*) as total_visit, rank() over(partition by name order by count(*) desc) as rank
	from entries
	group by name, floor  
	), distinct_res as
	(select name, resources as resc, count(*) as total_visit from entries 
	group by name, resources
	),distinct_string as
	(
	select name, STRING_AGG(resc,',')  as res_used 
	from distinct_res d 
	group by name
	)
	select h.name, t.total_floor as total_floor,h.floor,d.res_used
	from distinct_string d 
	join high_visit h on d.name=h.name
	join total_floor t on t.name=d.name
	where h.rank=1


--write a SQL Query to count no. of cancelled and completed request
	select request_at,
	count(case when status in ('cancelled_by_client' , 'cancelled_by_driver') then 1 else null end) as cancelled_count,
	count(*) as total_count,
	1.0*count(case when status in ('cancelled_by_client' , 'cancelled_by_driver') then 1 else null end) /count(*) as perc
	from trips t
	left join users u on t.client_id=u.users_id
	left join users d on t.driver_id=d.users_id
	where u.banned='No' and d.banned='No'
	group by request_at

--write a SQL Query to identify Tournament winners

	with total_matches as
	(
	select first_player as player, first_score as score from  matches
	union all
	select second_player as player,second_score as score  from  matches
	),player_rank as
	(select group_id,player_id,score,  
	rank() over(partition by group_id order by score desc, player_id asc) as rank 
	from  total_matches m join players p
	on m.player=p.player_id
	)
	select * from player_rank where rank=1
	order by group_id

-- write a sql query to find for each seller whether the brand of the 
--second item is favourite brand or not . if seller sold no two item show it as no o/p else show no/yes

	select * from  users

	select * from  orders

	select * from  items

	with seller_rank as
	(select *,dense_rank() over ( partition by seller_id order by order_date) as rank
	from  orders o)
	select u.user_id as seller_id,
	case when u.favorite_brand = i.item_brand then 'Yes' else 'No' end as second_item_favourite_brand from  users u
	left join seller_rank sr on sr.seller_id=u.user_id and rank=2
	left join items i on i.item_id=sr.item_id
	order by u.user_id


-- write a SQL Query to identify success and fail nos.

select * from  tasks

	with cte as
	(select *,   ROW_NUMBER() over (partition by state order by date_value) as rk
	,dateadd(day,-1* ROW_NUMBER() over (partition by state order by date_value),date_value) as new_group
	from  tasks)
	select new_group, state, min(date_value) as Min_Date, max(date_value)as Max_Date from cte
	group by new_group, state
	order by new_group

-- write a SQL Query to find the total number of users and
--the total amount spent using mobile only, desktop only and both mobile and desktop only and
--both mobile and desktop together for each date

   select * from spending 

	with cte as
	(select user_id as user_id, max(platform) as platform,sum(amount) as amount ,count(distinct platform) as count_platform
	from spending 
	group by user_id 
	having count(distinct platform)=1
	union all
	select user_id as user_id, max(platform) as platform,sum(amount) as amount ,count(distinct platform) as count_platform
	from spending 
	group by user_id 
	having count(distinct platform)=1
	) select distinct user_id, platform, amount, count_platform from cte
	union all

	select user_id,'both' as platform,sum(amount)as amount,count(distinct platform) as count_platform
	from spending
	group by user_id
	having count(distinct platform)=2
	order by user_id

--write a SQL Query to get EVERY ALPHABET IN ONE ROW---
	select *, SUBSTRING(ID,1,1) AS ALP from  INPUT

	with CTE(SN)AS
	( select 1 AS ID from INPUT
	UNION ALL
	select SN +1 AS ID from CTE where SN< (select LEN(ID) from INPUT)
	)
	select SN,ID, SUBSTRING(ID,SN,1) AS EXTRACT from CTE,INPUT

-- write a SQL Query to identify rating question----

	with CTE(SN)AS
	( select 1 AS ID from INPUT
	UNION ALL
	select SN +1 AS ID from CTE where SN< (select LEN(ID) from INPUT)
	) select replicate('*', SN) AS rating from cte

	------------------

	with cte1 as
	(select txndate, customername, row_number() over(order by (select null)) as rn
	from transactions
	), cte2 as
	(
	select *, lead(rn,1) over(order by rn) as next_rn from cte1 where customername is not null
	)
	--select * from cte2
	select cte2.customername, count(*) * 400 as subscription  from cte2 join cte1 on cte1.rn>=cte2.rn
	and (cte1.rn<=cte2.next_rn-1 or cte2.next_rn is null )
	group by cte2.customername


--write a sql query to  populate a column with last non null value.

	with cte1 as
	(select category, brand_name, row_number() over(order by (select null)) as rn
	from brands
	), cte2 as
	(select category, brand_name,rn , lead(rn,1) over(order by (rn)) as next_rn from cte1
	where category is not null
	)
	--select * from cte2
	select cte2.category, cte2.brand_name from cte2 join cte1 on cte1.rn>=cte2.rn 
	and (cte1.rn<=cte2.next_rn-1 or cte2.next_rn is null)

--Write a SQL Query to find products which are most frequently bought together 

	with cte as
	(
	select  o1.product_id as p1, o2.product_id as p2, count(o1.product_id) as Pur_cnt from orders1 o1 
	inner join orders1 o2
	on o1.order_id=o2.order_id where o2.product_id>o1.product_id
	group by  o1.product_id, o2.product_id
	)
	select  (pt1.name + pt2.name )as Grp, Pur_cnt
	from  cte c
	join products pt1 on c.p1=pt1.id
	join products pt2 on c.p2=pt2.id

-- WRITE A SQL QUERY TO IDENTIFY SECOND ACTIVITY FOR EACH USER--

	with CTE AS
	(select USERNAME, ACTIVITY, STARTDATE,ENDDATE,
	COUNT(*) OVER(PARTITION BY USERNAME) AS CNT, 
	RANK() OVER(PARTITION BY USERNAME order by STARTDATE DESC) AS RNK
	from UserActivity
	)

	select USERNAME, ACTIVITY, STARTDATE,ENDDATE
	from CTE where CNT=1 OR RNK =2

-- WRITE A SQL QUERY TO IDENTIFY EMPLOYEE with TOTAL CHARGES AS PER BILL DATE--

	with CTE AS
	(select EMP_NAME, BILL_RATE,BILL_DATE 
	,LEAD(DATEADD(DAY,-1,BILL_DATE),1,'9999-12-31') OVER(PARTITION BY EMP_NAME order by BILL_DATE)AS END_DATE
	from  billings 
	)  select  C.EMP_NAME,SUM(BILL_RATE * H.BILL_HRS )AS TOTALHOURS 
	--BILL_RATE,BILL_DATE, H.BILL_HRS,(BILL_RATE* H.BILL_HRS )AS TOTALHOURS 
	from CTE C INNER JOIN HoursWorked H ON C.EMP_NAME=H.EMP_NAME AND WORK_DATE BETWEEN C.BILL_DATE AND C.END_DATE
	group by C.EMP_NAME


--write a sql query to identify consecutives 3 empty seat no
	select * from  bms; 
	with CTE AS
	(select SEAT_NO, IS_EMPTY, 
	RANK() OVER(PARTITION BY IS_EMPTY order by SEAT_NO) AS RNK,
	(seat_no-RANK() OVER(PARTITION BY IS_EMPTY order by SEAT_NO) )as diff
	from  bms
	where IS_EMPTY='y'
	), cnt as
	(
	select diff, count(diff) as c_1 from CTE
	group by diff
	having count(diff)>=3

	)
	--select * from cte where diff in (select diff from cnt)
	select cte.SEAT_NO, cte.is_empty from cnt left join cte on cte.diff=cnt.diff

--write a sql query to Find Missing Quarter

	select store, concat('Q',10-sum(cast(right(quarter,1) as int))) as Q_name from STORES
	group by store;

--select * from STORES; 

	with CTE AS
	( select distinct STORE, 1 AS Q_NAME from STORES
	UNION ALL
	select STORE, Q_NAME+1 AS Q_NAME from  cte c  where Q_NAME<4
	), CTE2 AS
	(
	select STORE, 'Q'+cast(Q_NAME as char(1)) AS Q_NAME 
	from cte C 
	)
	select C2.STORE AS STORE,Q_NAME from CTE2 C2 LEFT JOIN STORES S ON S.STORE=C2.STORE AND S.QUARTER=C2.Q_NAME
	where S.STORE IS NULL
	order by STORE,Q_NAME

--write a sql query to Find STUDENTS HAVING SAME MARKS IN PHYSICS AND CHEMISTRY

	with STUDENT_MARKS AS
	(select STUDENT_ID,
	MAX(CASE WHEN SUBJECT ='CHEMISTRY' THEN MARKS END) AS C_MARKS,
	MAX(CASE WHEN SUBJECT ='pHYSICS' THEN MARKS END) AS P_MARKS
	--RANK() OVER(PARTITION BY STUDENT_ID  
	from exams
	group by STUDENT_ID
	)select DISTINCT E.STUDENT_ID
	--CASE WHEN C_MARKS=P_MARKS THEN 'SAME' ELSE 'DIFF' END AS SAME_DIFF_MARKS
	from STUDENT_MARKS SM JOIN EXAMS E ON E.STUDENT_ID=SM.STUDENT_ID
	where C_MARKS=P_MARKS

--write a sql query to Find daily increasing covid cases

	with city as
	(
	select CITY,
	rank() OVER (PARTITION BY CITY order by DAYS) AS days_rnk, 
	rank() OVER (PARTITION BY CITY order by cases) AS cases_rnk,
	rank() OVER (PARTITION BY CITY order by DAYS)-rank() OVER (PARTITION BY CITY order by cases) as diff
	from covid

	) select city from city
	group by city
	having sum( diff)=0 and count(distinct diff)=1


--write a sql query to Find companies who have atleast two users who speak english and german both the languages

	select * from company_users 

	with CTE AS
	(select company_id,user_id,
	MAX(case when language='English' then 'ENGLISH' END) AS E_Language,
	MAX(case when language='German' then 'GERMAN' END) AS G_Language
	from company_users
	group by company_id,user_id
	), ENGLISH AS
	(
	select COMPANY_ID,user_id, E_Language from CTE
	where E_LANGUAGE IS NOT NULL
	), GERMAN AS
	(
	select COMPANY_ID,user_id, G_Language from CTE
	where G_LANGUAGE IS NOT NULL
	) , company as
	(select E.company_id,E.USER_ID,G_LANGUAGE,E_LANGUAGE, 
	COUNT(*)OVER(PARTITION BY E.COMPANY_ID ) AS CNT
	from GERMAN G JOIN ENGLISH E ON E.COMPANY_ID=G.COMPANY_ID AND E.user_id=G.user_id
	group by E.company_id,E.USER_ID,G_LANGUAGE,E_LANGUAGE)
	select company_id,USER_ID
	from company
	where cnt>=2
	

--write a sql query to Find how many products fall into customer budgetalong with the list of products
-- in case of clash chosse the less costly product

	with running_total as
	(select product_id,cost, sum(cost) over(order by cost) as r_sum
	from products1 
	) select customer_id,budget, count(1) as total_cnt, STRING_AGG(product_id,',') as products from customer_budget cb  left join running_total rt
	on rt.r_sum<cb.budget
	group by customer_id,budget;

--write a sql query to Find total number of messages exchanged between each person per day

	select * from subscriber 

	with cte as
	(select sms_date,sms_no,
	case when sender<receiver then Sender else receiver  end as user1,
	case when sender>receiver then Sender else receiver end as user2
	from subscriber
	)
	select sms_date,user1, user2, sum(sms_no)as total_msgs from cte 
	group by sms_date,user1, user2;


--write a sql query to Find the largest order by value for each salesperson and display order details
-- get the answer without using cte, window function, sub query and temp tables


	select * from [int_orders]

	select a.order_number,a.order_date,a.cust_id,a.salesperson_id,a.amount
	from int_orders a join int_orders b
	on a.salesperson_id=b.salesperson_id
	group by a.order_number,a.order_date,a.cust_id,a.salesperson_id,a.amount
	HAVING A.AMOUNT>=MAX(B.AMOUNT)

--write a sql query to Find the on and off status as per time.

	select *from event_status

	with cte as
	(
	select *, lag( status,1,status) over(order by event_time) as prev_status
	from event_status
	),cte2 as
	(select event_time,status ,prev_status,sum(case when status='on' and prev_status ='off' then 1 else 0 end)over(order by event_time) as sm
	from cte
	group by event_time,status,prev_status
	) select min(event_time) as login, max(event_time) as logout, count(sm)-1 as cnt
	from cte2
	group by sm

--write a sql query to pivot the data from row to column

	select * from players_location

	with cte as
	(
	select *, row_number() over(partition by city order by name) as rn  from players_location
	) select 
	max(case when city='Bangalore' then name end) as Bangalore,
	max(case when city='Mumbai' then name end) as Mumbai,
	max(case when city='Delhi' then name end) as Delhi 
	from cte
	group by rn

--write a sql query to find median salary of employees for each company. 

	select * from employee 

	with cte as
	(select *, row_number() over(partition by company order by salary) as rn, 
	count(1) over(partition by company )as total_count
	from employee 
	)
	select *  from cte where rn between total_count*1.0/2 and total_count*1.0/2+1

--write a sql query to find 3rd highest salary employee in each department

	select * from emp

	with salary as
	(select emp_name,dep_id, salary,
	rank() over(partition by dep_id order by salary desc) as rnk,
	count(1) over(partition by dep_id) as cnt
	from emp
	)
	select * from salary where rnk=3 or(rnk<3 and rnk=cnt)

--write a sql query for human traffic of stadium.

	select * from stadium 

	with cte as
	(
	select *, row_number() over(order by visit_date) rn,
	(id-row_number() over(order by visit_date))as diff
	from stadium 
	where no_of_people>=100
	),cte2 as
	(
	select id,visit_date, no_of_people, diff, count(diff) over(partition by diff) as cnt
	from cte 
	group by id,visit_date, no_of_people,diff
	) select id,visit_date, no_of_people from cte2
	where cnt>3

--write a sql query to identify rating for Udaan business users.
	select * from business_city 


	with cte as
    (select datepart(year,business_date) as business_year , city_id
	from business_city 
	) select c1.business_year as business_year ,  count(c1.city_id) as cnt
	from cte c1 left join cte c2
	on c1.business_year>c2.business_year and c1.city_id=c2.city_id
	where c2.business_year is  null and c2.city_id is null
	group by c1.business_year 
