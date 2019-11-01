--20190913 由于添加按时段统计需求，表结构更改如下(只需执行一次)
alter table HT_WATER_STAT_DAY add h00_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h01_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h02_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h03_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h04_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h05_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h06_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h07_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h08_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h09_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h10_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h11_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h12_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h13_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h14_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h15_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h16_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h17_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h18_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h19_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h20_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h21_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h22_flow NUMBER(15,2);
alter table HT_WATER_STAT_DAY add h23_flow NUMBER(15,2);

comment on column HT_WATER_STAT_DAY.h00_flow is '0 时段用水';
comment on column HT_WATER_STAT_DAY.h01_flow is '1 时段用水';
comment on column HT_WATER_STAT_DAY.h02_flow is '2 时段用水';
comment on column HT_WATER_STAT_DAY.h03_flow is '3 时段用水';
comment on column HT_WATER_STAT_DAY.h04_flow is '4 时段用水';
comment on column HT_WATER_STAT_DAY.h05_flow is '5 时段用水';
comment on column HT_WATER_STAT_DAY.h06_flow is '6 时段用水';
comment on column HT_WATER_STAT_DAY.h07_flow is '7 时段用水';
comment on column HT_WATER_STAT_DAY.h08_flow is '8 时段用水';
comment on column HT_WATER_STAT_DAY.h09_flow is '9 时段用水';
comment on column HT_WATER_STAT_DAY.h10_flow is '10时段用水';
comment on column HT_WATER_STAT_DAY.h11_flow is '11时段用水';
comment on column HT_WATER_STAT_DAY.h12_flow is '12时段用水';
comment on column HT_WATER_STAT_DAY.h13_flow is '13时段用水';
comment on column HT_WATER_STAT_DAY.h14_flow is '14时段用水';
comment on column HT_WATER_STAT_DAY.h15_flow is '15时段用水';
comment on column HT_WATER_STAT_DAY.h16_flow is '16时段用水';
comment on column HT_WATER_STAT_DAY.h17_flow is '17时段用水';
comment on column HT_WATER_STAT_DAY.h18_flow is '18时段用水';
comment on column HT_WATER_STAT_DAY.h19_flow is '19时段用水';
comment on column HT_WATER_STAT_DAY.h20_flow is '20时段用水';
comment on column HT_WATER_STAT_DAY.h21_flow is '21时段用水';
comment on column HT_WATER_STAT_DAY.h22_flow is '22时段用水';
comment on column HT_WATER_STAT_DAY.h23_flow is '23时段用水';

--插入每日(昨日)水表数（此3个语句应设置于零晨之后，也可封装在一个过程内。由定时任务调起）
--由于表结构更新，相应逻辑更新如下
DELETE ht_water_stat_day WHERE acct_day = to_char(TRUNC(SYSDATE-1),'yyyymmdd');
INSERT INTO ht_water_stat_day(
  node_id, 
  ip_addr, 
  daily_flow, 
  month_flow, 
  year_flow, 
  accu_flow, 
  acct_day,
  h00_flow,
  h01_flow,
  h02_flow,
  h03_flow,
  h04_flow,
  h05_flow,
  h06_flow,
  h07_flow,
  h08_flow,
  h09_flow,
  h10_flow,
  h11_flow,
  h12_flow,
  h13_flow,
  h14_flow,
  h15_flow,
  h16_flow,
  h17_flow,
  h18_flow,
  h19_flow,
  h20_flow,
  h21_flow,
  h22_flow,
  h23_flow
) SELECT b.node_id, a.ip_address,
         SUM(CASE WHEN acct_type IN (0, -1) AND a.rn = 1 THEN water_flow END) daily_flow,
         SUM(CASE WHEN acct_type IN (0, -3) AND a.rn = 1 THEN water_flow END) month_flow,
         SUM(CASE WHEN acct_type IN (0, -6) AND a.rn = 1 THEN water_flow END) year_flow,
         SUM(CASE WHEN acct_type = 0 AND a.rn = 1 THEN water_flow END) accu_flow,
         to_char(SYSDATE-1,'yyyymmdd') acct_day,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '00' THEN water_flow END),0)+
         NVL(SUM(CASE WHEN acct_type = -1 AND collect_hour = '23' THEN water_flow END),0) h00_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '01' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '00' THEN water_flow END),0) h01_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '02' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '01' THEN water_flow END),0) h02_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '03' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '02' THEN water_flow END),0) h03_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '04' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '03' THEN water_flow END),0) h04_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '05' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '04' THEN water_flow END),0) h05_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '06' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '05' THEN water_flow END),0) h06_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '07' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '06' THEN water_flow END),0) h07_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '08' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '07' THEN water_flow END),0) h08_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '09' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '08' THEN water_flow END),0) h09_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '10' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '09' THEN water_flow END),0) h10_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '11' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '10' THEN water_flow END),0) h11_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '12' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '11' THEN water_flow END),0) h12_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '13' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '12' THEN water_flow END),0) h13_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '14' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '13' THEN water_flow END),0) h14_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '15' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '14' THEN water_flow END),0) h15_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '16' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '15' THEN water_flow END),0) h16_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '17' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '16' THEN water_flow END),0) h17_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '18' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '17' THEN water_flow END),0) h18_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '19' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '18' THEN water_flow END),0) h19_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '20' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '19' THEN water_flow END),0) h20_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '21' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '20' THEN water_flow END),0) h21_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '22' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '21' THEN water_flow END),0) h22_flow,
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '23' THEN water_flow END),0)-
         NVL(SUM(CASE WHEN acct_type = 0 AND collect_hour = '22' THEN water_flow END),0) h23_flow
    FROM (SELECT ip_address, SUBSTR(collect_time, 1, 10) collect_day, SUBSTR(collect_time, 12, 2) collect_hour,
                 DECODE(SUBSTR(collect_time,1,10),to_char(SYSDATE-1,'yyyy-mm-dd'),positive_flow_sum,-positive_flow_sum) water_flow,
                 CASE WHEN SUBSTR(collect_time, 1, 10) = to_char(SYSDATE-1,'yyyy-mm-dd') THEN 0
                      WHEN SUBSTR(collect_time, 1, 10) = to_char(SYSDATE-2,'yyyy-mm-dd') THEN -1
                      WHEN SUBSTR(collect_time, 1, 7) = to_char(add_months(SYSDATE-1,-1),'yyyy-mm') THEN -3
                      WHEN SUBSTR(collect_time, 1, 4) = to_char(SYSDATE-1,'yyyy')-1 THEN -6
                 END acct_type, --账期类型
                 row_number() OVER(PARTITION BY ip_address, SUBSTR(collect_time, 1, 10) ORDER BY collect_time DESC) rn,
				 row_number() OVER(PARTITION BY ip_address, SUBSTR(collect_time, 1, 13) ORDER BY collect_time DESC) rn1
            FROM ht_water_collection
           WHERE collect_time >= to_char(TRUNC(SYSDATE)-1,'yyyy-mm-dd hh24:mi:ss')
             AND collect_time < to_char(TRUNC(SYSDATE),'yyyy-mm-dd hh24:mi:ss')   --基准日
              OR collect_time >= to_char(TRUNC(SYSDATE)-2,'yyyy-mm-dd hh24:mi:ss')
             AND collect_time < to_char(TRUNC(SYSDATE)-1,'yyyy-mm-dd hh24:mi:ss') --基准前一日
              OR collect_time >= to_char(last_day(add_months(SYSDATE-1,-1)),'yyyy-mm-dd')
             AND collect_time < to_char(TRUNC(SYSDATE, 'month'),'yyyy-mm-dd')     --上月最后一天
              OR collect_time >= (to_char(SYSDATE-1,'yyyy')-1)||'-12-31'
             AND collect_time < to_char(TRUNC(SYSDATE-1, 'year'),'yyyy-mm-dd')    --去年最后一天
         ) a,
         (SELECT node_id, ip_addr, install_place FROM a_device) b
   WHERE a.ip_address = b.ip_addr AND a.rn1 = 1
   GROUP BY b.node_id, a.ip_address;
COMMIT;
