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
--20190913 由于添加按时段统计需求，表结构更改如下
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
         SUM(CASE WHEN to_char(SYSDATE-1, 'dd') = '01' AND acct_type IN (0, -1) AND a.rn = 1 THEN water_flow
                  WHEN acct_type IN (0, -3) AND a.rn = 1 THEN water_flow END) month_flow,
         SUM(CASE WHEN to_char(SYSDATE-1, 'mmdd') = '0101' AND acct_type IN (0, -1) AND a.rn = 1 THEN water_flow
                  WHEN acct_type IN (0, -6) AND a.rn = 1 THEN water_flow END) year_flow,
         SUM(CASE WHEN acct_type = 0 AND a.rn = 1 THEN water_flow END) accu_flow,
         to_char(SYSDATE-1, 'yyyymmdd') acct_day,
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
                 DECODE(SUBSTR(collect_time, 1, 10),to_char(SYSDATE-1,'yyyy-mm-dd'),positive_flow_sum,-positive_flow_sum) water_flow,
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
SELECT t.node_id, n.name node_name, p.month_plan, t.month_flow,
       CASE WHEN t.month_flow > p.month_plan THEN 1 ELSE 0 END is_over
  FROM (SELECT b.node_id,
               SUM(a.water_flow)-NVL(SUM(d.accu_flow),0)+NVL(SUM(d.month_flow),0) month_flow
          FROM (SELECT ip_address, positive_flow_sum water_flow,
                       row_number() OVER (PARTITION BY ip_address ORDER BY collect_time DESC) rn
                  FROM ht_water_collection
                 WHERE collect_time >= to_char(TRUNC(SYSDATE), 'yyyy-mm-dd')) a,
               (SELECT ip_addr, accu_flow,
                       (CASE WHEN substr(acct_day, 1, 6)=to_char(SYSDATE, 'yyyymm')
                             THEN month_flow else 0 END) month_flow
                  FROM ht_water_stat_day
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
          FROM a_water_plan WHERE YEAR = to_char(SYSDATE, 'yyyy')) p,
       a_node n
 WHERE t.node_id = p.node_id(+) AND t.node_id = n.id AND p.node_id = n.id
 ORDER BY n.weight;

--漏水逻辑（注意有2参数，由外部输入）
INSERT INTO a_warn_msg(
  type,
  msg,
  create_time,
  ip_addr
) SELECT sys_guid(),'2', w.node_id||'节点'||w.install_place||'漏水', ip_address
    FROM (SELECT ip_address,
                 SUM(CASE WHEN time_type=2 THEN positive_flow_sum
                          WHEN time_type=3 THEN -positive_flow_sum END) aim_flow,
                 SUM(CASE WHEN time_type=1 THEN positive_flow_sum
                          WHEN time_type=2 THEN -positive_flow_sum END) now_flow
            FROM (SELECT ip_address, positive_flow_sum, time_type,
                         row_number() OVER (PARTITION BY ip_address, time_type ORDER BY collect_time DESC) rn
                    FROM (SELECT ip_address, positive_flow_sum, collect_time,
                                 CASE WHEN SYSDATE-to_date(collect_time,'yyyy-mm-dd hh24:mi:ss')
                                           >=2*&INTERVAL_TIME/3600/24 THEN 3
                                      WHEN SYSDATE-to_date(collect_time,'yyyy-mm-dd hh24:mi:ss')
                                           >=&INTERVAL_TIME/3600/24 THEN 2
                                      ELSE 1 END time_type FROM ht_water_collection
                           WHERE collect_time >= to_char(SYSDATE-3*&INTERVAL_TIME/3600/24, 'yyyy-mm-dd hh24:mi:ss'))
                 ) WHERE rn = 1 GROUP BY ip_address) a,
         (SELECT node_id, ip_addr, install_place, leak_perc FROM a_device WHERE is_warn = '1') w
   WHERE a.ip_address = w.ip_addr AND a.now_flow < a.aim_flow*(1-w.leak_perc/100);
COMMIT;