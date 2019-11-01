SELECT t.node_id, p.month_plan, t.month_flow,
       CASE WHEN t.month_flow > p.month_plan THEN 1 ELSE 0 END is_over
  FROM (SELECT b.node_id,
               SUM(a.water_flow)-NVL(SUM(d.accu_flow),0)+NVL(SUM(d.month_flow),0) month_flow
          FROM (SELECT ip_address, positive_flow_sum water_flow,
                       row_number() OVER (PARTITION BY ip_address ORDER BY collect_time DESC) rn
                  FROM ht_water_collection
                 WHERE collect_time >= to_char(TRUNC(SYSDATE), 'yyyy-mm-dd')) a,
               (SELECT ip_addr, month_flow,
                       (CASE WHEN substr(acct_day, 1, 6)=to_char(SYSDATE - 1, 'yyyymm')
                            THEN accu_flow else 0 END) accu_flow
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
          FROM a_water_plan WHERE YEAR = to_char(SYSDATE, 'yyyy')) p
 WHERE t.node_id = p.node_id(+);