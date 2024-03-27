--write a SQL Query for Words which are repeating in the column-
	select value, count(*) as count_words from [master].[dbo].[nam_python]
	cross apply string_split(content,' ')
	group by value
	having count(*) >1
	order by count_words desc
 

--write a SQL Query to find the origin and destination of each cid---
	select o.cid,o.origin, d.destination as final_destination 
	from flights o  join 
	flights d on o.destination=d.origin



--write a SQL Query to find the count of each customer added in each month--

	with n_customer as
	(select order_date,customer, 
	ROW_NUMBER() over(partition by customer order by order_date) as rn
	from sales
	)
	select order_date,count(customer) as new_customer from n_customer where rn=1
	group by order_date


--write a sql query to find the ranking points table-
	 with total_matches as
	(select team_1 as team , count(*) as matches_played   from [dbo].[world_c] group by team_1
	union all
	select team_2 as team, count(*) as matches_played  from [dbo].[world_c] group by team_2
	), winners as
	(
	select winner as team, count(*) as winning_count
	from  world_c 
	group by winner
	)
	select tm.team, sum(tm.matches_played) as total_matches_played,coalesce(w.winning_count,0) as wins,
	sum(tm.matches_played)-coalesce(w.winning_count,0) as losses, (sum(tm.matches_played)-coalesce(w.winning_count,0))*2 as pts
	from total_matches tm left join winners w on w.team=tm.team
	group by tm.team, coalesce(w.winning_count,0)
------------2ND-----

	WITH TOTAL_MATCHES AS
	(SELECT TEAM_1 AS TEAMS, COUNT(*) AS TOTAL_TEAMS FROM  [dbo].[world_c] GROUP BY team_1
	UNION all
	SELECT TEAM_2 AS TEAMS , COUNT(*) AS TOTAL_TEAMS FROM  [dbo].[world_c] GROUP BY team_2
	), winning_matches as
	(
	select winner, count(*)as wins from [world_c] 
	group by winner
	)
	select tm.teams, sum(tm.TOTAL_TEAMS) as Total_Mathes_played, coalesce(wins,0) as wins,
	(sum(tm.TOTAL_TEAMS)-coalesce(wins,0)) as Losses,  
	2*(sum(tm.TOTAL_TEAMS)-coalesce(wins,0)) as Pts
	from  total_matches tm left join winning_matches wm
	on tm.teams=wm.winner
	group by tm.teams, coalesce(wins,0) 


--write a SQL Query to find the CHILD MOTHER FATHER RELATIONSHIOP IN table
	WITH F AS
	(SELECT R.C_ID  AS CHILD, P.NAME AS MOTHER_NAME FROM relations R
	JOIN PEOPLE P ON R.P_ID=P.ID WHERE GENDER='F'
	), M AS
	(SELECT R.C_ID  AS CHILD, P.NAME AS FATHER_NAME FROM relations R
	JOIN PEOPLE P ON R.P_ID=P.ID WHERE GENDER='M'
	), FM AS 
	(
	SELECT F.CHILD,MOTHER_NAME,FATHER_NAME FROM F INNER JOIN M ON F.CHILD=M.CHILD
	)
	SELECT NAME AS CHILD_NAME,MOTHER_NAME,FATHER_NAME FROM FM FM 
	JOIN PEOPLE P ON FM.CHILD=P.ID
	----- SECOND SOLUTION---
	WITH CHILD AS
	(SELECT R.C_ID  AS CHILD, MAX(M.NAME) AS MOTHER_NAME, MAX(F.NAME) AS FATHER_NAME 
	FROM relations R
	LEFT JOIN PEOPLE F ON R.P_ID=F.ID AND F.GENDER='M'
	LEFT JOIN PEOPLE M ON R.P_ID=M.ID AND M.GENDER='F'
	GROUP BY R.C_ID
	)
	SELECT NAME AS CHILD_NAME,MOTHER_NAME,FATHER_NAME  FROM CHILD C
	INNER JOIN PEOPLE P ON C.CHILD=P.ID


---find the COMPANY whose revenue increasing every year IN table-
	with cte as
	(select *,
	lag(revenue,1,0) over(partition by company order by year) as previous_rn,
	revenue-lag(revenue,1,0) over(partition by company order by year) as diff_rev,
	count(1) over(partition by company) as cnt
	from [dbo].[company_revenue]
	)
	select company-- ,cnt,count(1) as sales_inc_yrs 
	from cte
	where diff_rev >0 
	group by company,cnt
	having cnt=count(1)

---find the adult and child pair to go to fare--

	with adult as
	(
	select *,row_number() over(order by person) as rn from family where type='adult'
	), child as
	(select *,row_number() over(order by person) as rn  from family where type='child'
	)
	select a.person as Adult, c.person as Child from adult a left join child c on a.rn=c.rn

---find the criteria of players and team to enter

	select * from [dbo].[Ameriprise_LLC]

	with criteria_select as
	(select teamid--,criteria1, criteria2
	, (case when criteria1='Y' AND Criteria2='Y' THEN 'Y' ELSE 'N' END) AS Criteria
	,SUM( case when criteria1='Y' AND Criteria2='Y' THEN 1 ELSE 0 END) as count_no
	from [dbo].[Ameriprise_LLC]
	group by teamid,(case when criteria1='Y' AND Criteria2='Y' THEN 'Y' ELSE 'N' END)
	--HAVING COUNT ( case when criteria1='Y' AND Criteria2='Y' THEN 'Y' ELSE 'N' END) >=2 
	), total as
	(select teamID, Criteria1, Criteria2,
	(case when criteria1='Y' AND Criteria2='Y' THEN 'Y' ELSE 'N' END) AS Criteria
	from [Ameriprise_LLC]
	)
	select T.teamID,Criteria1,Criteria2,CS.Criteria,
	(case when  CS.CRITERIA='Y' AND COUNT_NO>=2 THEN 'Y' ELSE 'N' END )AS FINAL_CRITERIA_FLAG 
	from total t left join criteria_select cs 
	on  t.teamID=cs.teamID and t.Criteria=cs.criteria


--find the value on the basis of formulas--
	select * from input

	with Value1 as
	(select i.id, left(i.formula,1) as d1,substring(i.formula,2,1) mid,right(i.formula,1) as d2, i.value, i.formula--, f1.value,f2.id, f2.value
	from input i 
	)
	select value1.id, value1.formula, ip1.value as ip1value, ip2.value  as ip2value,
	case when value1.mid='+' then ip1.value + ip2.value else  ip1.value - ip2.value end as final_value
	from value1
	inner join input ip1 on ip1.id=value1.id
	inner join input ip2 on ip2.id=value1.d2

--write a query to find start and end time of call from two tables--
	select * from  call_start_logs
	select * from call_end_logs
	
	with start_log as
	(
	select phone_number,min(start_time) as start_time, row_number() over(partition by phone_number order by start_time asc) as rn_start
	from  call_start_logs
	group by phone_number
	), end_log as
	(
	select phone_number,max(End_time) as End_time, row_number() over(partition by phone_number order by end_time asc) as rn_end
	from  call_end_logs
	phone_number
	)
	select s.phone_number, start_time,End_time, DATEDIFF(min,start_time,End_time) as diff from start_log s join end_log e on s.rn_start=e.rn_end

--write a query to print highest and lowest salary of empp in each department---

	with cte as
	(
	select  dep_id,min(salary) as min_sal, max(salary) as max_sal
	from employee
	group by dep_id
	)
	SELECT CTE.dep_id,
	MAX(CASE WHEN SALARY=min_sal THEN EMP_NAME END) AS MIN_EMP_NAME,
	MAX(CASE WHEN SALARY=max_sal THEN EMP_NAME END) AS MAX_EMP_NAME
	 FROM employee E INNER JOIN CTE ON CTE.dep_id=E.dep_id
	GROUP BY CTE.dep_id


-- write a query to find no. of gold medals per swimmer for swimmers who won gold medal--

	select gold, COUNT(*) AS NO_OF_MEDALS from events 
	where gold not in (SELECT SILVER FROM EVENTS UNION SELECT BRONZE FROM EVENTS)
	GROUP BY GOLD

--WRITE A SQL QUERY TO FIND BUSINESS DAY BETWEEN CREATE DAY AND RESOLVED DATE BY EXCLUDING WEEKENDS AND PUBLIC HOLIDAYS-

---buinessdays are total days-weekends-holidays---

	SELECT ticket_id,create_date, resolved_date,datediff(day,create_date,resolved_date) as total_days,
	datediff(day,create_date,resolved_date) -(2*datediff(week,create_date,resolved_date))total_week_days,
	count(holiday_date) as total_Holidays, 
	datediff(day,create_date,resolved_date) -(2*datediff(week,create_date,resolved_date))-count(holiday_date) as total_business_days
	FROM tickets t
	left join holidays h on holiday_date between create_date and resolved_date
	group by ticket_id,create_date, resolved_date

--WRITE A SQL QUERY TO FIND THE total number of people inside the Hospital.

	select emp_id, 
	max(case when action ='in' then time end) as in_type,
	max(case when action ='out' then time end) as out_type
	from hospital 
	group by emp_id
	having max(case when action ='in' then time end)>max(case when action ='out' then time end) or 
	max(case when action ='out' then time end) is null

---2ND ---
	with in_time as
	(
	select emp_id, max(time) as max_in_time
	from hospital
	where action='in'
	group by emp_id
	), out_time as
	(
	select emp_id, max(time) as max_out_time
	from hospital
	where action='out'
	group by emp_id
	)
	select in_time.emp_id,max_in_time,max_out_time
	from in_time left join out_time on in_time.emp_id=out_time.emp_id
	where max_in_time >max_out_time OR max_out_time IS NULL

--WRITE A SQL QUERY TO convert comma segrerated file--

	select value as room_type, count(value) as no_of_searches from airbnb_searches 
	cross apply string_split(filter_room_types,',')
	group by value
	order by no_of_searches desc

--WRITE A SQL QUERY TO identify all employee whose salary is same in same department --


	with salary as
	(select dept_id, salary, count(*) as no_of_sal
	from emp_salary
	group by dept_id, salary
	having count(*)>1
	)
	select es. emp_id,es.name,es.salary, es.dept_id 
	from emp_salary es join salary s on es.dept_id=s.dept_id and es.salary=s.salary

