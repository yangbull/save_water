--����ÿ���ۻ�����
CREATE TABLE SAVE_WATER.HT_WATER_STAT_DAY 
   (node_id    VARCHAR2(20),
	ip_addr    VARCHAR2(15),
	daily_flow NUMBER(16,2),
	month_flow NUMBER(16,2),
	year_flow  NUMBER(18,2),
	accu_flow  NUMBER(20,2),
	acct_day   CHAR(8)
   ) SEGMENT CREATION IMMEDIATE;

COMMENT ON COLUMN ht_water_stat_day.node_id    IS '�����ڵ�';
COMMENT ON COLUMN ht_water_stat_day.ip_addr    IS 'ˮ��ip';
COMMENT ON COLUMN ht_water_stat_day.daily_flow IS '������';
COMMENT ON COLUMN ht_water_stat_day.month_flow IS '������';
COMMENT ON COLUMN ht_water_stat_day.year_flow  IS '������';
COMMENT ON COLUMN ht_water_stat_day.accu_flow  IS '�ۼ�����(�������)';
COMMENT ON COLUMN ht_water_stat_day.acct_day   IS 'ͳ����';

--����ÿ��(����)ˮ��������3�����Ӧ�������㳿֮��Ҳ�ɷ�װ��һ�������ڡ��ɶ�ʱ�������
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
                 END acct_type, --��������
                 row_number() OVER(PARTITION BY ip_address, SUBSTR(collect_time, 1, 10) ORDER BY collect_time DESC) rn
            FROM ht_water_collection
           WHERE collect_time >= to_char(TRUNC(SYSDATE)-1,'yyyy-mm-dd hh24:mi:ss')
             AND collect_time < to_char(TRUNC(SYSDATE),'yyyy-mm-dd hh24:mi:ss')   --��׼��
              OR collect_time >= to_char(TRUNC(SYSDATE)-2,'yyyy-mm-dd hh24:mi:ss')
             AND collect_time < to_char(TRUNC(SYSDATE)-1,'yyyy-mm-dd hh24:mi:ss') --��׼ǰһ��
              OR collect_time >= to_char(last_day(add_months(SYSDATE-1,-1)),'yyyy-mm-dd')
             AND collect_time < to_char(TRUNC(SYSDATE, 'month'),'yyyy-mm-dd')      --�������һ��
              OR collect_time >= (to_char(SYSDATE-1,'yyyy')-1)||'-12-31'
             AND collect_time < to_char(TRUNC(SYSDATE-1, 'year'),'yyyy-mm-dd')     --ȥ�����һ��
         ) a,
         (SELECT node_id, ip_addr, install_place FROM a_device) b
   WHERE a.ip_address = b.ip_addr AND a.rn = 1
   GROUP BY b.node_id, a.ip_address;
COMMIT;

--�û���ѯ��ʽ
SELECT to_char(to_date(acct_day,'yyyymmdd'), 'yyyy-mm-dd') acct_day,
       SUM(DECODE(node_id,'2',daily_flow)), --����
       SUM(DECODE(node_id,'3',daily_flow)), --����¥
       SUM(DECODE(node_id,'4',daily_flow)), --ʳ��
       SUM(DECODE(node_id,'5',daily_flow)), --���¥
       SUM(DECODE(node_id,'6',daily_flow)), --������ϴ�ּ�
       SUM(DECODE(node_id,'7',daily_flow)), --����Ůϴ�ּ�
       SUM(DECODE(node_id,'8',daily_flow)), --��ů��ˮ
       SUM(DECODE(node_id,'9',daily_flow)), --14/15¥����
       SUM(DECODE(node_id,'10',daily_flow)), --8¥������ˮ
       SUM(DECODE(node_id,'11',daily_flow)) --�յ���ȴ��
  FROM ht_water_stat_day
 WHERE acct_day BETWEEN REPLACE(&beg_day, '-', NULL)
   AND REPLACE(&end_day, '-', NULL)
 GROUP BY acct_day;

--ҳ���ѯ�����
SELECT a.ip_address, b.node_id, b.install_place,
       SUM(decode(collect_year,to_char(SYSDATE, 'yyyy'),positive_flow_sum,-positive_flow_sum)) year_flow,
       MAX(CASE WHEN TRUNC(a.diff_time*86400) > &err_time THEN 0 ELSE 1 END) is_bad
  FROM (SELECT ip_address, positive_flow_sum, SUBSTR(collect_time, 1, 4) collect_year,
                SYSDATE - to_date(collect_time, 'yyyy-mm-dd hh24:mi:ss') diff_time,
                row_number() OVER (PARTITION BY ip_address, SUBSTR(collect_time, 1, 4) ORDER BY collect_time DESC) rn
          FROM ht_water_collection
         WHERE SUBSTR(collect_time, 1, 10) = to_char(SYSDATE, 'yyyy-mm-dd') --'2019-08-24'
               OR SUBSTR(collect_time, 1, 4) = to_char(SYSDATE, 'yyyy')-1) a, --��Ҫ���Ǳ����ۼ���
       (SELECT node_id, ip_addr, install_place FROM a_device) b
 WHERE a.ip_address = b.ip_addr AND a.rn = 1
 GROUP BY a.ip_address, b.node_id, b.install_place;

--������ˮ����ʵʱ��/��ƻ���ˮ��/ƽ����ˮ
SELECT SUM(CASE WHEN ip_address NOT IN ('1.1.1.7', '1.1.1.8', '1.1.1.9')
	             THEN decode(collect_year,to_char(SYSDATE, 'yyyy'),positive_flow_sum,-positive_flow_sum)
            END) year_flow, --������ˮӦȥ������3���Ӽ�ˮ��
       SUM(CASE WHEN ip_address NOT IN ('1.1.1.7', '1.1.1.8', '1.1.1.9')
                 THEN c.year_plan END) year_plan, --�ƻ���ˮҲ��ȥ������3���Ӽ�ˮ��
       ROUND(SUM(CASE WHEN ip_address NOT IN ('1.1.1.6', '1.1.1.11', '1.1.1.7', '1.1.1.8', '1.1.1.9')
	                   THEN decode(collect_year,to_char(SYSDATE, 'yyyy'),positive_flow_sum,-positive_flow_sum)
                  END)/SUM(b.water_users), 2) avg_water --ƽ����ˮ������ʳ�ü�������3���Ӽ�ˮ��
  FROM (SELECT ip_address, positive_flow_sum, SUBSTR(collect_time, 1, 4) collect_year,
                row_number() OVER (PARTITION BY ip_address, SUBSTR(collect_time, 1, 4) ORDER BY collect_time DESC) rn
          FROM ht_water_collection
         WHERE SUBSTR(collect_time, 1, 4) IN (to_char(SYSDATE, 'yyyy'), to_char(SYSDATE, 'yyyy')-1)
       ) a,
	   (SELECT SUM(VALUE) water_users FROM a_dict WHERE TYPE = 'user_num') b,
	   (SELECT SUM(year_plan) year_plan FROM a_water_plan t WHERE YEAR=to_char(SYSDATE, 'yyyy')) c
 WHERE rn = 1;

--���굱����ˮ������״̬���������
SELECT a.node_id, a.month_plan, b.water_flow
  FROM (SELECT node_id,
               CASE to_char(SYSDATE+120, 'mm')
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
          FROM a_water_plan t WHERE YEAR=to_char(SYSDATE, 'yyyy')) a,
       (SELECT b.node_id, SUM(a.water_flow) water_flow
          FROM (SELECT ip_address, positive_flow_sum water_flow,
                       row_number() OVER (PARTITION BY ip_address ORDER BY collect_time DESC) rn
                  FROM ht_water_collection
                 WHERE SUBSTR(collect_time, 1, 7) = to_char(SYSDATE, 'yyyy-mm')
                ) a,
                (SELECT node_id, ip_addr, install_place FROM a_device) b
         WHERE a.ip_address = b.ip_addr AND a.rn = 1
         GROUP BY b.node_id) b
 WHERE a.node_id = b.node_id(+);

--©ˮ�߼���ע����2���������ⲿ���룩
--       CASE WHEN miner_flow<=0 THEN 0 --ͣ
--            WHEN miner_flow<aim_flow THEN -1 --©
--            ELSE 1 END exp_flag --����

SELECT '-1' TYPE, NULL receive_userid, w.install_place||'©ˮ', ip_address,
  FROM (SELECT ip_address,
			   SUM(decode(time_type,1,positive_flow_sum,-positive_flow_sum)) miner_flow,
			   SUM(decode(time_type,2,positive_flow_sum))*&perc_base aim_flow
		  FROM (
				SELECT ip_address, positive_flow_sum, time_type,
					   row_number() OVER (PARTITION BY ip_address, time_type ORDER BY collect_time DESC) rn
				  FROM (SELECT ip_address, positive_flow_sum, collect_time,
								CASE WHEN SYSDATE-to_date(collect_time,'yyyy-mm-dd hh24:mi:ss')
										   >=&inter_time/3600/24 THEN 2 ELSE 1 END time_type
						  FROM ht_water_collection
						 WHERE collect_time >= to_char(SYSDATE-1/24, 'yyyy-mm-dd hh24:mi:ss')
					   )
			   )
		 WHERE rn = 1
		 GROUP BY ip_address
       ) a, (SELECT node_id, install_place, ip_addr FROM a_device WHERE is_warn = 1) w
 WHERE a.ip_address = w.ip_addr AND miner_flow<aim_flow;

select * from ORGMODEL_USERINFO where STS=1