--This calculates the number of rounds that have a change of driver in the leading position.
create or replace view laps_with_change_pos_1_v
as
    select raceid, count(*) laps
    from
    (
        select raceid, lap
        from
        (
            select raceid, position, lap, driverid as current_driver,
              LAG (driverid, 1) OVER (PARTITION BY raceid, position ORDER BY lap) AS prev_driver
            from lap_times
        )
        where prev_driver is not null
        and prev_driver <> current_driver
        and position = 1
        group by raceid, lap
    )
    group by raceid;

--This calculates the number of rounds that have a change of driver in the top 5 positions.
create or replace view laps_with_change_pos_1_5_v
as
    select raceid, count(*) laps
    from
    (
        select raceid, lap, count(*)
        from
        (
            select raceid, position, lap, driverid as current_driver,
              LAG (driverid, 1) OVER (PARTITION BY raceid, position ORDER BY lap) AS prev_driver
            from lap_times
        )
        where prev_driver is not null
        and prev_driver <> current_driver
        and (position >= 1 and position <= 5)
        group by raceid, lap
        order by raceid, lap
    )
    group by raceid
    order by raceid;

--This calculates the number of positions that a driver has lost during a lap. It separates those position changes in actual take overs and take overs that were a result of pit stops.
create or replace view overtaken_positions_total
as
    select raceid, overtaken_positions_due_to_pitstop_total, (overtaken_positions_total - overtaken_positions_due_to_pitstop_total) as overtaken_positions_real_total
    from
    (
        select raceid, sum(overtaken_positions_driver_lap) as overtaken_positions_total, sum(overtaken_positions_driver_lap_due_to_pitstop) as overtaken_positions_due_to_pitstop_total
        from
        (
            select raceid, driverid, lap, overtaken_positions_driver_lap, nvl2(pitstop_raceid, overtaken_positions_driver_lap,0 ) overtaken_positions_driver_lap_due_to_pitstop
            from
            (
                select ot.raceid, ot.driverid, ot.lap, sum(ot.overtaken_this_lap) as overtaken_positions_driver_lap, ps.raceid pitstop_raceid
                from
                (
                    select ottl.raceid, ottl.driverid, ottl.lap, (ottl.current_position - ottl.prev_position) as overtaken_this_lap
                    from
                    (
                        select lt.raceid, lt.driverid, lt.lap, lt.position as current_position, 
                          LAG (lt.position, 1) OVER (PARTITION BY lt.raceid, lt.driverid ORDER BY lt.lap) AS prev_position
                        from lap_times lt
                    ) ottl
                    where ottl.prev_position is not null
                    and ottl.prev_position < ottl.current_position
                )   ot
                ,   pit_stops ps
                where ps.raceid (+) = ot.raceid
                and   ps.driverid (+) = ot.driverid
                and   ps.lap (+) = ot.lap
                group by ot.raceid, ot.driverid, ot.lap, ps.raceid
            )
        )
        group by raceid
    );

--The following calculates the come back score for a race. This is defined that the maximum distance that one of the top finishing drivers (positions 1-8) has recovered from a position back in the field. E.g. when a driver came back from position 18 to position 1, the come back score is 17. 
create or replace view comebackscores
as
    select ps.raceid, max(ps.driver_comebackscore) max_comebackscore
    from (
        select raceid, driverid, max(delta_position) as driver_comebackscore
        from
        (
            select lt.raceid, lt.driverid, (ltbefore.position - lt.position) delta_position
            from lap_times lt
            , lap_times ltbefore
            where ltbefore.raceid = lt.raceid
            and ltbefore.driverid = lt.driverid
            and ltbefore.lap < lt.lap
        )
        group by raceid, driverid) ps
    ,   results r
    where r.raceid = ps.raceid
    and r.position in ('1', '2', '3', '4', '5', '6', '7', '8')
    group by ps.raceid;


--This calculates the difference between the ranking of the driver and his finishing position. For example, if a driver is ranked 20th and finishes 1st, this adds 19 to this metric. This can be seen as a measure of "surprise".
create or replace view position_difference_top_5_v
as
    select result_raceid as raceid, sum(position_difference_top_5) as position_difference_top_5
    from
    (
        select result_raceid, abs(position_result - position_ranking) position_difference_top_5
        from
        (
            select ra.round, s.raceid ranking_raceid, r.raceid result_raceid, r.driverid, r.position position_result, nvl(s.position, 0) position_ranking
            from   driver_standings s
            ,      results r
            ,      races ra
            where  s.raceid  = (r.raceid - 1)
            and    ra.raceid = r.raceid
            and    s.driverid  = r.driverid
            and    r.position in ('1', '2', '3', '4', '5')
            order by r.raceid
        )
    )
    group by result_raceid;


-- 
alter table races add laps_with_change_pos_1 number;
alter table races add laps_with_change_pos_1_5 number;
alter table races add overtaken_positions_due_to_pitstop_total number;
alter table races add overtaken_positions_real_total number;
alter table races add max_comebackscore number;
alter table races add rank_versus_position number;
alter table races add safety_car number;

MERGE INTO races e USING (SELECT * FROM laps_with_change_pos_1_v) h
        ON (e.raceid = h.raceid)
      WHEN MATCHED THEN
        UPDATE SET e.laps_with_change_pos_1 = h.laps;

MERGE INTO races e USING (SELECT * FROM laps_with_change_pos_1_5_v) h
        ON (e.raceid = h.raceid)
      WHEN MATCHED THEN
        UPDATE SET e.laps_with_change_pos_1_5 = h.laps;

MERGE INTO races e USING (SELECT * FROM overtaken_positions_total) h
        ON (e.raceid = h.raceid)
      WHEN MATCHED THEN
        UPDATE SET e.overtaken_positions_due_to_pitstop_total = h.overtaken_positions_due_to_pitstop_total
        ,          e.overtaken_positions_real_total = h.overtaken_positions_real_total;

MERGE INTO races e USING (SELECT * FROM comebackscores) h
        ON (e.raceid = h.raceid)
      WHEN MATCHED THEN
        UPDATE SET e.max_comebackscore = h.max_comebackscore;

MERGE INTO races e USING (SELECT * FROM position_difference_top_5_v) h
        ON (e.raceid = h.raceid)
      WHEN MATCHED THEN
        UPDATE SET e.rank_versus_position = h.position_difference_top_5;


MERGE INTO races e USING (SELECT * FROM safety_car) h
        ON (e.name = h.race AND e.year = h.year)
      WHEN MATCHED THEN
        UPDATE SET e.safety_car = 1;

update races set laps_with_change_pos_1 = 0 where laps_with_change_pos_1 is null;
update races set laps_with_change_pos_1_5 = 0 where laps_with_change_pos_1_5 is null;
update races set max_comebackscore = 0 where max_comebackscore is null;
update races set rank_versus_position = 0 where rank_versus_position is null;
update races set safety_car = 0 where safety_car is null;


--Split the dataset
--Create a dedicated RACES_TRAIN table. We will use this later to TRAIN the model on the years until 2019.
--We will use the year 2020 to TEST the model later.
create table races_train as select * from races where year >= 2011 and year <= 2019; --Exclude 2020





