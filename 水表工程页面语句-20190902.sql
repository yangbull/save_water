--创建每日累积表数
CREATE TABLE HT_WATER_STAT_DAY 
   (node_id    VARCHAR2(20),
    ip_addr    VARCHAR2(15),
    daily_flow NUMBER(16,2),
    month_flow NUMBER(16,2),
    year_flow  NUMBER(18,2),
    accu_flow  NUMBER(20,2),
    acct_day   CHAR(8)
   ) SEGMENT CREATION IMMEDIATE;

COMMENT ON COLUMN ht_water_stat_day.node_id    IS '所属节点';
COMMENT ON COLUMN ht_water_stat_day.ip_addr    IS '水表ip';
COMMENT ON COLUMN ht_water_stat_day.daily_flow IS '日流量';
COMMENT ON COLUMN ht_water_stat_day.month_flow IS '月流量';
COMMENT ON COLUMN ht_water_stat_day.year_flow  IS '年流量';
COMMENT ON COLUMN ht_water_stat_day.accu_flow  IS '累计流量(物理读数)';
COMMENT ON COLUMN ht_water_stat_day.acct_day   IS '统计期';

--插入每日(昨日)水表数（此3个语句应设置于零晨之后，也可封装在一个过程内。由定时任务调起）
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
             AND collect_time < to_char(TRUNC(SYSDATE, 'month'),'yyyy-mm-dd')     --上月最后一天
              OR collect_time >= (to_char(SYSDATE-1,'yyyy')-1)||'-12-31'
             AND collect_time < to_char(TRUNC(SYSDATE-1, 'year'),'yyyy-mm-dd')    --去年最后一天
         ) a,
         (SELECT node_id, ip_addr, install_place FROM a_device) b
   WHERE a.ip_address = b.ip_addr AND a.rn = 1
   GROUP BY b.node_id, a.ip_address;
COMMIT;

--用户查询格式(时间段横表)
SELECT to_char(to_date(acct_day,'yyyymmdd'), 'yyyy-mm-dd') acct_day,
       SUM(DECODE(node_id,'2',daily_flow)), --高区
       SUM(DECODE(node_id,'3',daily_flow)), --服务楼
       SUM(DECODE(node_id,'4',daily_flow)), --食堂
       SUM(DECODE(node_id,'5',daily_flow)), --监测楼
       SUM(DECODE(node_id,'6',daily_flow)), --低区男洗手间
       SUM(DECODE(node_id,'7',daily_flow)), --低区女洗手间
       SUM(DECODE(node_id,'8',daily_flow)), --供暖补水
       SUM(DECODE(node_id,'9',daily_flow)), --14/15楼大便池
       SUM(DECODE(node_id,'10',daily_flow)), --8楼以上用水
       SUM(DECODE(node_id,'11',daily_flow)) --空调冷却塔
  FROM ht_water_stat_day
 WHERE acct_day BETWEEN REPLACE(&beg_day, '-', NULL)
   AND REPLACE(&end_day, '-', NULL)
 GROUP BY acct_day;

--页面查询表读数（非历史累积读数，乃本年累计）
SELECT a.ip_address,    --ip地址
       b.node_id,       --节点
       b.install_place, --安装位置
       a.positive_flow_sum-NVL(c.accu_flow,0)+NVL(c.year_flow,0) year_flow, --本年累计
       CASE WHEN TRUNC(a.diff_time*86400) > &err_time THEN 1 ELSE 0 END is_bad --1:故障 0:正常
  FROM (SELECT ip_address, positive_flow_sum,
               SYSDATE-to_date(collect_time, 'yyyy-mm-dd hh24:mi:ss') diff_time,
               row_number() OVER (PARTITION BY ip_address ORDER BY collect_time DESC) rn
          FROM ht_water_collection
         WHERE collect_time >= to_char(TRUNC(SYSDATE), 'yyyy-mm-dd')) a,
       (SELECT node_id, ip_addr, install_place FROM a_device) b,
       (SELECT ip_addr, year_flow, accu_flow FROM ht_water_stat_day
         WHERE acct_day = to_char(SYSDATE-1, 'yyyymmdd')) c
 WHERE a.ip_address = c.ip_addr(+) AND a.ip_address = b.ip_addr AND a.rn = 1;

--本年用水量（实时）/年计划用水量/平均用水
SELECT SUM(t.year_flow) year_flow, --本年用水应去掉高区3个子级水表
       SUM(c.year_plan) year_plan, --计划用水也得去掉高区3个子级水表
       ROUND(SUM(CASE WHEN t.node_id NOT IN (4) THEN year_flow END)
             /NULLIF(SUM(b.water_users),0), 2) avg_water --平均用水不包括食堂及高区的3个子级水表
  FROM (SELECT e.node_id, SUM(a.positive_flow_sum-NVL(d.accu_flow,0)+NVL(d.year_flow,0)) year_flow
          FROM (SELECT ip_address, positive_flow_sum,
                       row_number() OVER (PARTITION BY ip_address ORDER BY collect_time DESC) rn
                  FROM ht_water_collection
                 WHERE collect_time >= to_char(TRUNC(SYSDATE), 'yyyy-mm-dd')) a,
               (SELECT ip_addr, year_flow, accu_flow FROM ht_water_stat_day
                 WHERE acct_day = to_char(SYSDATE-1, 'yyyymmdd')) d,
               (SELECT ip_addr, node_id FROM a_device WHERE node_id NOT IN (6, 7 ,8)) e
        WHERE a.ip_address = d.ip_addr(+) AND a.ip_address = e.ip_addr AND a.rn = 1
        GROUP BY e.node_id) t,
       (SELECT VALUE water_users FROM a_dict WHERE TYPE = 'user_num') b,
       (SELECT node_id, year_plan FROM a_water_plan WHERE YEAR = to_char(SYSDATE, 'yyyy')) c
 WHERE t.node_id = c.node_id

--当年当月用水分析
SELECT t.node_id, p.month_plan, t.month_flow,
       CASE WHEN t.month_flow > p.month_plan THEN 1 ELSE 0 END is_over
  FROM (SELECT b.node_id,
               SUM(a.water_flow)-NVL(SUM(d.accu_flow),0)+NVL(SUM(d.month_flow),0) month_flow
          FROM (SELECT ip_address, positive_flow_sum water_flow,
                       row_number() OVER (PARTITION BY ip_address ORDER BY collect_time DESC) rn
                  FROM ht_water_collection
                 WHERE collect_time >= to_char(TRUNC(SYSDATE), 'yyyy-mm-dd')) a,
               (SELECT ip_addr, month_flow, accu_flow FROM ht_water_stat_day
				 WHERE acct_day = to_char(SYSDATE - 1, 'yyyymmdd')) d,
               (SELECT node_id, ip_addr FROM a_device) b
         WHERE a.ip_address = d.ip_addr(+) AND a.ip_address = b.ip_addr AND a.rn = 1
         GROUP BY b.node_id) t,
       (SELECT node_id,
               CASE to_char(SYSDATE, 'mm')
                 WHEN '01' THEN jan_plan
                 WHEN '02' THEN feb_plan
                 WHEN '03' THEN mar_plan
                 WHEN '04' THEN apr_plan
                 WHEN '05' THEN may_plan
                 WHEN '06' THEN june_plan
                 WHEN '07' THEN july_plan
                 WHEN '08' THEN aug_plan
                 WHEN '09' THEN sept_plan
                 WHEN '10' THEN oct_plan
                 WHEN '11' THEN nov_plan
                 WHEN '12' THEN dec_plan
               END month_plan
          FROM a_water_plan WHERE YEAR = to_char(SYSDATE, 'yyyy')) p
 WHERE t.node_id = p.node_id(+);

--漏水逻辑（注意有2参数，由外部输入）
INSERT INTO a_warn_msg(
  type, msg, create_time, ip_addr
) SELECT '-1' TYPE, w.node_id||'节点'||w.install_place||'漏水', sysdate, ip_address
    FROM (SELECT ip_address, SUM(decode(time_type,2,positive_flow_sum)) aim_flow,
                 SUM(decode(time_type,1,positive_flow_sum,-positive_flow_sum)) miner_flow
            FROM (SELECT ip_address, positive_flow_sum, time_type,
                         row_number() OVER (PARTITION BY ip_address, time_type ORDER BY collect_time DESC) rn
                    FROM (SELECT ip_address, positive_flow_sum, collect_time,
                                 CASE WHEN SYSDATE-to_date(collect_time,'yyyy-mm-dd hh24:mi:ss')
                                   >=&interval_time/3600/24 THEN 2 ELSE 1 END time_type
                            FROM ht_water_collection
                           WHERE collect_time >= to_char(SYSDATE-1/24, 'yyyy-mm-dd hh24:mi:ss'))
                 ) WHERE rn = 1 GROUP BY ip_address) a,
         (SELECT node_id, ip_addr, install_place, leak_perc FROM a_device WHERE is_warn = 1) w
   WHERE a.ip_address = w.ip_addr AND a.miner_flow < a.aim_flow*w.leak_perc/100;
COMMIT;