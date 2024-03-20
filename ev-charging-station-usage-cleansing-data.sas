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
	format start_time_of_day $9. end_time_of_day $9. energy_consumption_level $6. ghg_saving_level $6. gasoline_saving_level $6.;

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

	*month of charging time;
	if month(datepart(start_date)) in (1,2,3) then charging_month="Winter";
	else if month(datepart(start_date)) in (4,5,6) then charging_month="Spring";
	else if month(datepart(start_date)) in (7,8,9) then charging_month="Summer";
	else if month(datepart(start_date)) in (10,11,12) then charging_month="Autumn";

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
run;
