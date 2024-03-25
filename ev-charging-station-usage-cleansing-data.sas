*counting missing values;
proc sql;
	select nmiss(transaction_date) as transaction_date, nmiss(port_type) as port_type, 
		   nmiss(evse_id) as evse_id, nmiss(currency) as currency, nmiss(ended_by) as ended_by, 
		   nmiss(driver_postal_code) as driver_postal_code, nmiss(user_id) as user_id
	from casuser.ev_charging_station_usage;
quit;

*format fixes;
data casuser.ev_charging_station_clean;
	format station_name $30. mac_address $19. org_name $17. plug_type $10. evse_id 8. address1 $19. fee 8. ended_by $28.; 
	set casuser.ev_charging_station_usage;
	end_date=put(input(end_date, anydtdtm.), DATETIME16.);
	transaction_date=put(input(transaction_date, anydtdtm.), DATETIME16.);
	mac_address= compress(mac_address, ":");
	if station_name="PALO ALTO CA / BRYANT # 1" then station_name="PALO ALTO CA / BRYANT #1";
run;

*filling missing values with matched values;
proc sql;
	*filling currency value;
    create table temp_curr as
    	select distinct user_id, currency from casuser.ev_charging_station_clean
    	where currency is not null;
    update casuser.ev_charging_station_clean t1
    	set currency = (select t2.currency from temp_curr t2 where t2.user_id = t1.user_id)
    	where currency is null;
	
	*filling evse_id value;
	create table temp_evse as
    	select distinct mac_address, evse_id from casuser.ev_charging_station_clean 
    	where evse_id is not null;
    update casuser.ev_charging_station_clean t1
    	set evse_id = (select t2.evse_id from temp_evse t2 where t2.mac_address = t1.mac_address)
    	where evse_id is null;

	drop table temp_evse, temp_curr;
quit;

*removing missing values;
data casuser.ev_charging_station_clean;
	set casuser.ev_charging_station_clean;
	where driver_postal_code <> . and currency<>"" and ended_by<>"" and user_id <> "" and transaction_date <> "" and port_type <> "" ;
run;

*counting missing values;
proc sql;
	select nmiss(transaction_date) as transaction_date, nmiss(port_type) as port_type, 
		   nmiss(evse_id) as evse_id, nmiss(currency) as currency, nmiss(ended_by) as ended_by, 
		   nmiss(driver_postal_code) as driver_postal_code, nmiss(user_id) as user_id
	from casuser.ev_charging_station_clean;
quit;

*transformation numerical variables and removing outlier values;
proc means data=casuser.ev_charging_station_clean n nmiss mean std min q1 median q3 max skewness;
	var energy_kWh GHG_savings_kg gasoline_savings_gallons fee;
run;

data casuser.ev_charging_station_clean;
	set casuser.ev_charging_station_clean;
	energy_kWh_log=log(energy_kWh +1);
	GHG_savings_kg_log=log(GHG_savings_kg +1);
	gasoline_savings_gallons_sqrt=sqrt(gasoline_savings_gallons);
	fee_sqrt=sqrt(fee);
run;

proc means data=casuser.ev_charging_station_clean n nmiss mean std min q1 median q3 max skewness;
	var energy_kWh_log GHG_savings_kg_log gasoline_savings_gallons_sqrt fee_sqrt;
run;

*feature extraxting;
data casuser.ev_charging_station_clean;
	set casuser.ev_charging_station_clean;
	format start_time_of_day $9. end_time_of_day $9. charging_month $9. energy_consumption_level $6. ghg_saving_level $6. gasoline_saving_level $6.;

	*start time of day;
	if hour(start_date) in (6,7,8,9,10,11) then start_time_of_day="Morning";
	else if hour(start_date) in (12,13,14,15,16) then start_time_of_day="Afternoon";
	else if hour(start_date) in (17,18,19,20,21) then start_time_of_day="Evening";
	else if hour(start_date) in (22,23,0,1,2,3,4,5) then start_time_of_day="Night";
	
	*start time of day;
	if hour(input(end_date, anydtdtm.)) in (6,7,8,9,10,11) then end_time_of_day="Morning";
	else if hour(input(end_date, anydtdtm.)) in (12,13,14,15,16) then end_time_of_day="Afternoon";
	else if hour(input(end_date, anydtdtm.)) in (17,18,19,20,21) then end_time_of_day="Evening";
	else if hour(input(end_date, anydtdtm.)) in (22,23,0,1,2,3,4,5) then end_time_of_day="Night";

	*days of charging time;
	select(weekday(datepart(start_date)));
      	when(1) day="Monday";
      	when(2) day="Tuesday";
		when(3) day="Wednesday";
		when(4) day="Thursday";
      	when(5) day="Friday";
		when(6) day="Saturday";
		when(7) day="Sunday";
   	end;

	*weekdays/weekend;
	if weekday(datepart(start_date)) in (1,2,3,4,5) then do;
		weekday=1;
		weekend=0;
	end;
	else do;
		weekday=0;
		weekend=1;
	end;

	*season of charging time;
	if month(datepart(start_date)) in (1,2,3) then charging_season="Winter";
	else if month(datepart(start_date)) in (4,5,6) then charging_season="Spring";
	else if month(datepart(start_date)) in (7,8,9) then charging_season="Summer";
	else if month(datepart(start_date)) in (10,11,12) then charging_season="Autumn";
		
	*months of charging time;
	select(month(datepart(start_date)));
      	when(1) charging_month="January";
      	when(2) charging_month="February";
		when(3) charging_month="March";
		when(4) charging_month="April";
      	when(5) charging_month="May";
		when(6) charging_month="June";
		when(7) charging_month="July";
		when(8) charging_month="August";
		when(9) charging_month="September";
      	when(10) charging_month="October";
		when(11) charging_month="November";
		when(12) charging_month="December";
   	end;

	*energy per hour;
	if hour(charging_time) = 0 then do;
		if minute(charging_time) = 0 then energy_per_hour=energy_kWh_log/(second(charging_time)/3600);
		else energy_per_hour=energy_kWh_log/(minute(charging_time)/60);
	end;
	else energy_per_hour=energy_kWh_log/hour(charging_time);

	*energy consumption level;
	if energy_kWh_log<=1.5656950 then energy_consumption_level="Low";
	else if energy_kWh_log>1.5656950 and energy_kWh_log<=2.5230852 then energy_consumption_level="Medium";
	else energy_consumption_level="High";

	*ghg saving level;
	if GHG_savings_kg_log<=0.9516579 then ghg_saving_level="Low";
	else if GHG_savings_kg_log>0.9516579 and GHG_savings_kg_log<=1.7606127 then ghg_saving_level="Medium";
	else ghg_saving_level="High";

	*gasoline saving level;
	if gasoline_savings_gallons_sqrt<=0.6892024 then gasoline_saving_level="Low";
	else if gasoline_savings_gallons_sqrt>0.6892024 and gasoline_savings_gallons_sqrt<=1.1995833 then gasoline_saving_level="Medium";
	else gasoline_saving_level="High";

	*station infos;
	station_phone="888-758-4389";
	access_code="Public";
	access_days_time="24 hours daily";
	ev_network="ChargePoint Network";
	geocode_status="GPS";

	where year(datepart(start_date)) in (2018, 2019) and fee<>0;
run;
