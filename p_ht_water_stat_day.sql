DELETE ht_water_stat_day WHERE acct_day = to_char(TRUNC(SYSDATE-1),'yyyymmdd');
INSERT INTO ht_water_stat_day(
  node_id, 
  ip_addr, 
  daily_flow, 
  month_flow, 
  year_flow, 
  accu_flow, 
  acct_day
) SELECT b.node_id, a.ip_address,
         SUM(CASE WHEN acct_type IN (0, -1) THEN water_flow END) daily_flow,
         SUM(CASE WHEN acct_type IN (0, -3) THEN water_flow END) month_flow,
         SUM(CASE WHEN acct_type IN (0, -6) THEN water_flow END) year_flow,
         SUM(CASE WHEN acct_type = 0 THEN water_flow END) accu_flow,
         to_char(SYSDATE-1,'yyyymmdd') acct_day
    FROM (SELECT ip_address, SUBSTR(collect_time, 1, 10) collect_day,
                 DECODE(SUBSTR(collect_time,1,10),to_char(SYSDATE-1,'yyyy-mm-dd'),positive_flow_sum,-positive_flow_sum) water_flow,
                 CASE WHEN SUBSTR(collect_time, 1, 10) = to_char(SYSDATE-1,'yyyy-mm-dd') THEN 0
                      WHEN SUBSTR(collect_time, 1, 10) = to_char(SYSDATE-2,'yyyy-mm-dd') THEN -1
                      WHEN SUBSTR(collect_time, 1, 7) = to_char(add_months(SYSDATE-1,-1),'yyyy-mm') THEN -3
                      WHEN SUBSTR(collect_time, 1, 4) = to_char(SYSDATE-1,'yyyy')-1 THEN -6
                 END acct_type, --账期类型
                 row_number() OVER(PARTITION BY ip_address, SUBSTR(collect_time, 1, 10) ORDER BY collect_time DESC) rn
            FROM ht_water_collection
           WHERE collect_time >= to_char(TRUNC(SYSDATE)-1,'yyyy-mm-dd hh24:mi:ss')
             AND collect_time < to_char(TRUNC(SYSDATE),'yyyy-mm-dd hh24:mi:ss')   --基准日
              OR collect_time >= to_char(TRUNC(SYSDATE)-2,'yyyy-mm-dd hh24:mi:ss')
             AND collect_time < to_char(TRUNC(SYSDATE)-1,'yyyy-mm-dd hh24:mi:ss') --基准前一日
              OR collect_time >= to_char(last_day(add_months(SYSDATE-1,-1)),'yyyy-mm-dd')
             AND collect_time < to_char(TRUNC(SYSDATE, 'month'),'yyyy-mm-dd')      --上月最后一天
              OR collect_time >= (to_char(SYSDATE-1,'yyyy')-1)||'-12-31'
             AND collect_time < to_char(TRUNC(SYSDATE-1, 'year'),'yyyy-mm-dd')     --去年最后一天
         ) a,
         (SELECT node_id, ip_addr, install_place FROM a_device) b
   WHERE a.ip_address = b.ip_addr AND a.rn = 1
   GROUP BY b.node_id, a.ip_address;
COMMIT;
