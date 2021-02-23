<?php
require_once('Main_model.php');

class TestCode {

    protected function enc($id,$name){
        return " CONVERT(VARCHAR(1000), {$id}, 1) AS {$name} ";
    }

    function get_debt_by_user($id,$start_date = null,$end_date = null,$sum = null){
        $p0 = "_Posted != 0x00 ";
        $df = $start_date && $end_date ? " AND CONVERT(varchar, DATEADD(year, -2000, dj._Date_Time), 20) >= '{$start_date}' AND CONVERT(varchar, DATEADD(year, -2000, dj._Date_Time), 20) <= DATEADD(day,1,'{$end_date}')"  : "";
        $sql = "SELECT * FROM (SELECT {$this->enc("dj._DocumentRRef","ID")},
                    {$this->enc("doc._IDRRef","sale_ID")},
                    {$this->enc("d131._IDRRef","purch_ID")},
                    {$this->enc("d21._IDRRef","inv_ID")},
                    CONVERT(varchar, DATEADD(year, -2000, dj._Date_Time), 20) AS payment_date,
                    r6._Description AS buyer,
                    dj._Number AS code,
                    ISNULL(new_price.pr,dj._Fld6677) as price,
                    NULL AS entry_amount,NULL as exit_amount, NULL as title,
                    dj._Fld6686 as comment,
                    ISNULL(r76._Description,'0') AS department,
                    d3._Description AS currency,
                    doc._Fld8937 AS bns,
                    d52._Description as contract,
                    CASE WHEN r6._ParentIDRRef != 0xA43568F728F0380D11E8D6CD6D31F20A
                    THEN 0
                    ELSE 1
                    END AS parent,
                    {$this->enc("dj._DocumentTRef","tref")},
                    CASE WHEN d225._IDRRef IS NOT NULL
                    THEN 1
                    ELSE 0
                    END as stat,
                    CONVERT(varchar, DATEADD(year, -2000, d220._Date_Time), 20) as paid_date,
                    CASE WHEN YEAR(doc._Fld5524) != '2001' THEN CONVERT(varchar, DATEADD(year, -2000, doc._Fld5524), 20) ELSE NULL END AS due_date
                FROM _DocumentJournal6674 dj
                OUTER APPLY (SELECT TOP 1 _Fld8385 as pr
                            FROM _AccumRg8379
                            WHERE _RecorderRRef = dj._DocumentRRef
                            ) as new_price
                OUTER APPLY (SELECT TOP 1 _Date_Time
                            FROM _Document220 d220_in
                            WHERE d220_in._Fld5237_RRRef = dj._DocumentRRef) as d220
                LEFT JOIN _Reference69 r6 ON r6._IDRRef = dj._Fld6675_RRRef
                LEFT JOIN _AccumRg8602 nullval ON nullval._RecorderRRef = dj._DocumentRRef AND nullval._RecorderTRef = 0x000000E1
                LEFT JOIN _Document131 d131 ON d131._IDRRef = dj._DocumentRRef
                LEFT JOIN _Document227 doc ON doc._IDRRef = dj._DocumentRRef
                LEFT JOIN _Document218 d21 ON d21._IDRRef = dj._DocumentRRef
                LEFT JOIN _Reference83 r76 ON r76._IDRRef = dj._Fld6679RRef
                LEFT JOIN _Reference52 d52 ON d52._IDRRef = dj._Fld6682_RRRef
                LEFT JOIN _Reference35 d3 ON d3._IDRRef = CASE WHEN d52._IDRRef IS NOT NULL THEN d52._Fld671RRef ELSE dj._Fld6678RRef END
                LEFT JOIN _Document225 d225 ON d225._IDRRef = dj._DocumentRRef AND d225._Fld5408_TYPE = 0x01 AND d225.{$p0}
                WHERE dj._Fld6683 IS NOT NULL
                AND dj._Fld6675_RRRef = {$id}
                AND dj.{$p0}
                AND nullval._RecorderRRef IS NULL
                {$df}
                UNION ALL
                SELECT CONVERT(VARCHAR(1000), acc._RecorderRRef, 1) AS ID,
                        NULL AS sale_ID, NULL AS purch_ID, NULL AS inv_ID,
                        CONVERT(varchar, DATEADD(year, -2000, MAX(acc._Period)), 20) as payment_date,
                        NULL AS buyer,MAX(nm._Number) as code,
                        NULL AS price,
                        MAX(CASE WHEN acc._RecordKind = 0
                        THEN acc._Fld7764
                        ELSE NULL END) AS entry_amount,
                        MAX(CASE WHEN acc._RecordKind = 1
                        THEN acc._Fld7764
                        ELSE NULL END) AS exit_amount,
                        MAX(nm._Fld9048) as title,
                        NULL AS comment,
                        '0' AS department,NULL AS currency,NULL AS bns,
                        MAX(cont._Description) as contract,NULL AS parent,
                        CONVERT(VARCHAR(1000), MAX(acc._RecorderTRef), 1)  as tref,NULL AS stat,
                        NULL AS paid_date,NULL AS due_date
                FROM _AccumRg7758 acc
                LEFT JOIN _Document9047 nm ON nm._IDRRef = acc._RecorderRRef
                LEFT JOIN _Reference52 cont ON cont._IDRRef = acc._Fld7759RRef
                WHERE acc._Fld7762RRef = {$id}
                AND acc._RecorderTRef = 0x00002357
                GROUP BY acc._RecorderRRef) AS payments
                ORDER BY payment_date";
        $arr = $this->remote_db->query($sql)->result_array();

        $realdb = new Main_model;
        $bonus = $realdb->get_bonus($id,1);
        $contracts = [];
        $payments = [];$detail = [];
        foreach ($arr as $i => $cont) {
        if ($cont['contract'] && !in_array($cont['contract'],$contracts)) {$contracts[] = $cont['contract'];}
        }
        $cr_list = [];
        foreach ($contracts as $k => $contract) {
        $list = [];
        foreach ($arr as $i => $sale) {
            $tr = str_replace('0x000000','',$sale['tref']);
            $pcase = $sale['parent'] == 1;
            $arr1 = $pcase ? ['E3','83','E1'] : ['E3','83'];
            $arr2 = $pcase ? ['83','84'] : ['83','84','E1'];
            $pr1 = in_array($tr,$arr1) ? (in_array($tr,$arr2) ? ((int)$sale['price'] < 0 ? $sale['price'] : '-'.$sale['price']) : $sale['price']) : '';
            $pr2 = !in_array($tr,$arr1) ? (in_array($tr,$arr2) ? ((int)$sale['price'] < 0 ? $sale['price'] : '-'.$sale['price']) : $sale['price']) : '';

            if (($sale['contract'] === null && $k == 0) || $sale['contract'] == $contract) {
            $list[] = [
                'ID' => $sale['ID'],
                'sale_ID' => $sale['sale_ID'],
                'purch_ID' => $sale['purch_ID'],
                'inv_ID' => $sale['inv_ID'],
                'payment_date' => $sale['payment_date'],
                'code' => $sale['code'],
                'currency' => $sale['currency'],
                'price_1' => $sale['entry_amount'] ? $sale['entry_amount'] : $pr1,
                'price_2' => $sale['exit_amount'] ? $sale['exit_amount'] : $pr2,
                'title' => $sale['title'],
                'mess' =>  in_array($tr,['E3','DA']) ? 2 : (in_array($tr,['DC','E1']) ? 1 : 3),
                'comment' => $sale['comment'],
                'department' => $sale['department'],
                'bns' => $sale['bns'],
                'contract' => $sale['contract'] ? $sale['contract'] : null,
                'tref' => $tr,
                'due_date' => $sale['due_date'] ? str_replace(' 00:00:00','',$sale['due_date']) : NULL,
                'paid_date' => $sale['paid_date'],
                'parent' => $sale['parent'],
                'stat' => $sale['stat'],
            ];
            $cr_list = array_merge($cr_list,array($contract => $sale['currency']));
            }
        }

        $detail = ['currencies' => $cr_list,'buyer' => $sale['buyer']];
        $payments = array_merge($payments,array($contract => $list));
        }
        return ['contracts' => $contracts,'detail' => $detail,'payments' => $payments,'bonus' => $bonus];
     }



    function get_debt_list($parent_list = null,$k = null,$bl=null,$cb = null,$start_date = null,$end_date = null,$tp = null,$minimum = null,$order_by =null,$limit_pass = null,$late_payment = null){
        $arr1 = " (0x000000E3,0x00000083) ";
        $arr2 = " (0x00000083,0x00000084,0x000000E1) ";
        $arr1_tech = " (0x000000E3,0x00000083,0x000000E1)  ";
        $arr2_tech = " (0x00000083,0x00000084) ";
        $if_tech = " cust._ParentIDRRef = 0xA43568F728F0380D11E8D6CD6D31F20A ";
        $djo = " djo._DocumentTRef ";

        $left_debt = (int)$cb;


        $d1 = " "; $d2 = " "; $d3 = " ";
        if (!$start_date || !$end_date) {return [];}
        $d1 = " WHERE sale_date >= '{$start_date}' AND sale_date <= dateadd(day,1,'{$end_date}')";
        $d2 = " AND djo._Date_Time >= CONVERT(varchar, DATEADD(year, +2000, '{$start_date}'), 20) AND djo._Date_Time <= dateadd(day,1,CONVERT(varchar, DATEADD(year, +2000, '{$end_date}'), 20)) ";
        $d3 =  "(djo._Date_Time >= CONVERT(varchar, DATEADD(year, +2000, '{$start_date}'), 20) AND djo._Date_Time <= dateadd(day,1,CONVERT(varchar, DATEADD(year, +2000, '{$end_date}'), 20)))";
        $second_pmt_date_query =  "(_Period >= CONVERT(varchar, DATEADD(year, +2000, '{$start_date}'), 20) AND _Period <= dateadd(day,1,CONVERT(varchar, DATEADD(year, +2000, '{$end_date}'), 20)))";

        $left_debt_query = "";

        if (in_array($left_debt,[0,1])) {
        $lft_bnc_nm = " ISNULL((pays.sum + ISNULL(left_balance.entry_amt,0)) - (pays.sum2 + ISNULL(left_balance.exit_amt,0)),0) ";
        if (!$left_debt) {
            $left_debt_query = " AND $lft_bnc_nm = 0 AND ISNULL(other_acc.amount,0) = 0 ";
        }else{
            $left_debt_query = " AND ($lft_bnc_nm > 0 OR $lft_bnc_nm < 0 OR ISNULL(other_acc.amount,0) > 0 OR ISNULL(other_acc.amount,0) < 0) ";
        }
        }

        $keyword_query = $k ? " AND (cust._Description LIKE '%{$k}%' OR cust._Code LIKE '%{$k}%' OR CAST(inf._Fld7091 AS varchar(8000)) LIKE '%{$k}%')" : "";
        $late_payment_query = $late_payment ? " AND (YEAR(ls_pymt.date) != YEAR(getdate()) OR MONTH(ls_pymt.date) != MONTH(getdate())) " : "";
        $name_start_with = $bl ? " AND cust._Description LIKE '{$bl}%' " : '';
        $parent_query = $parent_list ? " AND cust._ParentIDRRef = {$parent_list} " : '';


        $sql = "SELECT CONVERT(VARCHAR(1000), cust._IDRRef, 1) as ID,cust._Description as cust_name,cust._Code as code,cust._Fld802 as description,
                                    CAST(inf._Fld7091 AS varchar(8000)) as phone_numb,parent._Description as parent,parent._Code as pr_code,
                                    ISNULL(other_acc.amount,0) as left_amount_in_currency,other_acc.tp as curr_type,
                ls_pymt.date AS last_payment,
                ISNULL((SELECT TOP 1 _Fld8604 as bns
                        FROM _AccumRgT8605 WHERE _Fld8603RRef = cust._IDRRef
                        AND _Splitter = CASE WHEN YEAR(_Period) > 5000 THEN 0 ELSE 1 END ORDER BY _Period DESC),0) as bonus,
                ISNULL(pays.sum + ISNULL(left_balance.entry_amt,0),0) as entry_amount,
                ISNULL(pays.sum2 + ISNULL(left_balance.exit_amt,0),0) as exit_amount,
                ISNULL((pays.sum + ISNULL(left_balance.entry_amt,0)) - (pays.sum2 + ISNULL(left_balance.exit_amt,0)),0) as left_amount,
                left_balance.entry_amt as left_balance_entry,
                left_balance.exit_amt as left_balance_exit,
                ISNULL(pays.sum3 + ISNULL(left_balance.entry_amt_2,0),0) as entry_amount_2,
                ISNULL(pays.sum4 + ISNULL(left_balance.exit_amt_2,0),0) as exit_amount_2,
                ISNULL((pays.sum3 + ISNULL(left_balance.entry_amt_2,0)) - (pays.sum4 + ISNULL(left_balance.exit_amt_2,0)),0) as left_amount_2
                FROM _Reference69 cust
                OUTER APPLY (SELECT TOP 1 CONVERT(varchar, DATEADD(year, -2000, djo._Date_Time), 20) as date FROM _DocumentJournal6741 djo
                                WHERE djo._Fld6745_RRRef = cust._IDRRef {$d2} ORDER BY djo._Date_Time DESC) as ls_pymt
                OUTER APPLY (SELECT TOP 1 acc._Fld7763 as amount,
                                    CASE WHEN rr._Fld671RRef = 0xA43568F728F0380D11E8D6CB31F65910 THEN '$'
                                    WHEN rr._Fld671RRef = 0xA43568F728F0380D11E8D6CB31F65911 THEN '€' ELSE '' END as tp
                                FROM _AccumRgT7765 acc
                                LEFT JOIN _Reference52 rr ON rr._IDRRef = acc._Fld7759RRef
                                WHERE acc._Fld7762RRef = cust._IDRRef
                                AND rr._Fld671RRef != 0xA43568F728F0380D11E8D6CB31F6590E
                                ORDER BY _Period DESC) as other_acc
                OUTER APPLY (SELECT SUM(price) AS sum,SUM(price_2) AS sum2,SUM(price_3) AS sum3,SUM(price_4) AS sum4
                            FROM (SELECT
                                        CASE WHEN {$if_tech}
                                        THEN
                                        CASE WHEN {$djo} IN {$arr1_tech} THEN
                                            CASE
                                                WHEN {$djo} IN {$arr2_tech} THEN
                                                (CASE WHEN ISNULL(new_price.amt,djo._Fld6677) < 0
                                                THEN ISNULL(new_price.amt,djo._Fld6677)
                                                ELSE 0 - ISNULL(new_price.amt,djo._Fld6677)
                                                END)
                                            ELSE ISNULL(new_price.amt,djo._Fld6677) END
                                        ELSE 0 END
                                        ELSE
                                        CASE WHEN {$djo} IN {$arr1} THEN
                                            CASE
                                                WHEN {$djo} IN {$arr2} THEN
                                                (CASE WHEN ISNULL(new_price.amt,djo._Fld6677) < 0
                                                THEN ISNULL(new_price.amt,djo._Fld6677)
                                                ELSE 0 - ISNULL(new_price.amt,djo._Fld6677)
                                                END)
                                            ELSE ISNULL(new_price.amt,djo._Fld6677) END
                                        ELSE 0 END
                                        END AS price,

                                        CASE WHEN {$if_tech}
                                        THEN
                                        CASE WHEN {$djo} NOT IN {$arr1_tech} THEN
                                            CASE
                                                WHEN {$djo} IN {$arr2_tech} THEN
                                                (CASE WHEN ISNULL(new_price.amt,djo._Fld6677) < 0
                                                THEN ISNULL(new_price.amt,djo._Fld6677)
                                                ELSE 0 - ISNULL(new_price.amt,djo._Fld6677)
                                                END)
                                            ELSE ISNULL(new_price.amt,djo._Fld6677) END
                                        ELSE 0 END
                                        ELSE
                                        CASE WHEN {$djo} NOT IN {$arr1} THEN
                                            CASE
                                                WHEN {$djo} IN {$arr2} THEN
                                                (CASE WHEN ISNULL(new_price.amt,djo._Fld6677) < 0
                                                THEN ISNULL(new_price.amt,djo._Fld6677)
                                                ELSE 0 - ISNULL(new_price.amt,djo._Fld6677)
                                                END)
                                            ELSE ISNULL(new_price.amt,djo._Fld6677) END
                                        ELSE 0 END
                                        END AS price_2,

                                        CASE WHEN {$if_tech}
                                        THEN
                                        CASE WHEN {$d3} THEN
                                            CASE WHEN {$djo} IN {$arr1_tech}
                                            THEN
                                            CASE WHEN {$djo} IN {$arr2_tech}
                                            THEN
                                                (CASE WHEN ISNULL(new_price.amt,djo._Fld6677) < 0
                                                THEN ISNULL(new_price.amt,djo._Fld6677)
                                                ELSE 0 - ISNULL(new_price.amt,djo._Fld6677)
                                                END)
                                            ELSE ISNULL(new_price.amt,djo._Fld6677) END
                                            ELSE 0 END
                                        ELSE 0 END
                                        ELSE
                                        CASE WHEN {$d3} THEN
                                            CASE WHEN {$djo} IN {$arr1}
                                            THEN
                                            CASE WHEN {$djo} IN {$arr2}
                                            THEN
                                                (CASE WHEN ISNULL(new_price.amt,djo._Fld6677) < 0
                                                THEN ISNULL(new_price.amt,djo._Fld6677)
                                                ELSE 0 - ISNULL(new_price.amt,djo._Fld6677)
                                                END)
                                            ELSE ISNULL(new_price.amt,djo._Fld6677) END
                                            ELSE 0 END
                                        ELSE 0 END
                                        END AS price_3,
                                        CASE WHEN {$if_tech}THEN
                                        CASE WHEN {$d3} THEN
                                            CASE WHEN {$djo} NOT IN {$arr1_tech}
                                            THEN
                                            CASE WHEN {$djo} IN {$arr2_tech}
                                            THEN
                                                (CASE WHEN ISNULL(new_price.amt,djo._Fld6677) < 0
                                                THEN ISNULL(new_price.amt,djo._Fld6677)
                                                ELSE 0 - ISNULL(new_price.amt,djo._Fld6677) END)
                                            ELSE ISNULL(new_price.amt,djo._Fld6677) END
                                            ELSE 0 END
                                        ELSE 0 END
                                        ELSE
                                        CASE WHEN {$d3} THEN
                                            CASE WHEN {$djo} NOT IN {$arr1}
                                            THEN
                                            CASE WHEN {$djo} IN {$arr2}
                                            THEN
                                                    (CASE WHEN ISNULL(new_price.amt,djo._Fld6677) < 0
                                                    THEN ISNULL(new_price.amt,djo._Fld6677)
                                                    ELSE 0 - ISNULL(new_price.amt,djo._Fld6677) END)
                                            ELSE ISNULL(new_price.amt,djo._Fld6677) END
                                            ELSE 0 END
                                        ELSE 0 END
                                        END AS price_4
                                        FROM _DocumentJournal6674 djo
                                        OUTER APPLY (SELECT TOP 1 _Fld8385 as amt
                                                    FROM _AccumRg8379
                                                    WHERE _RecorderRRef = djo._DocumentRRef
                                                    ) as new_price
                                        LEFT JOIN _AccumRg8602 nullval ON nullval._RecorderRRef = djo._DocumentRRef AND nullval._RecorderTRef = 0x000000E1
                                        LEFT JOIN _Reference52 d52 ON d52._IDRRef = djo._Fld6682_RRRef
                                        WHERE djo._Fld6675_RRRef = cust._IDRRef
                                        AND djo._Fld6683 IS NOT NULL
                                        AND djo._Posted != 0x00
                                        AND nullval._RecorderRRef IS NULL) as payments) as pays
                OUTER APPLY (SELECT
                                MAX(CASE WHEN acc._RecordKind = 0
                                THEN acc._Fld7764
                                ELSE NULL END) as entry_amt,
                                MAX(CASE WHEN acc._RecordKind = 0 AND $second_pmt_date_query
                                THEN acc._Fld7764
                                ELSE NULL END) as entry_amt_2,
                                MAX(CASE WHEN acc._RecordKind = 1
                                THEN acc._Fld7764
                                ELSE NULL END) as exit_amt,
                                MAX(CASE WHEN acc._RecordKind = 1 AND $second_pmt_date_query
                                THEN acc._Fld7764
                                ELSE NULL END) as exit_amt_2
                            FROM _AccumRg7758 acc
                            WHERE acc._Fld7762RRef = cust._IDRRef
                            AND acc._RecorderTRef = 0x00002357
                            GROUP BY acc._RecorderRRef) as left_balance
                LEFT JOIN _Reference69 parent ON parent._IDRRef = cust._ParentIDRRef
                LEFT JOIN _InfoRg7087 inf ON inf._Fld7088_RRRef = cust._IDRRef
                WHERE cust._Folder = 0x01
                {$parent_query}
                {$name_start_with}
                {$left_debt_query}
                {$keyword_query}
                {$late_payment_query}
                ".($order_by ? "ORDER BY cust._Description ASC" : "ORDER BY cust._Code");

        $debt_mssql = $this->remote_db->query($sql);
        $debts = [];
        $rows = $debt_mssql->result_array();
        $sum_1 = 0;$sum_2 = 0;$sum_3 = 0;
        $sumInPeriod1 = 0;
        $sumInPeriod2 = 0;
        foreach ($rows as $key => $row) {
        if (!$tp) {
            $customers = $this->local_db->where('code',$row['ID']);
            $cus = $this->local_db->get('customers')->result_array();
        }
        $lmt = isset($cus[0]['lmt']) ? $cus[0]['lmt'] : '';
        if ($minimum) {
            $debts[] = [
            'ID' => $row['ID'],
            'cust_name' => $row['cust_name'],
            'code' => $row['code'],
            'left_amount' => (float)$row['left_amount'],
            'last_payment' => $row['last_payment'],
            'left_amount_in_currency' => $row['left_amount_in_currency'],
            'limit' => $lmt,
            ];
        }else{
            if (($limit_pass && $lmt && floatval($lmt) < floatval($row['left_amount'])) || !$limit_pass) {
            $debts[] = [
                'ID' => $row['ID'],
                'left_balance_entry' => $row['left_balance_entry'],
                'left_balance_exit' => $row['left_balance_exit'],
                'cust_name' => $row['cust_name'],
                'code' => $row['code'],
                'description' => $row['description'],
                'phone_numb' => $row['phone_numb'],
                'parent' => $row['parent'],
                'curr_type' => $row['curr_type'],
                'pr_code' => $row['pr_code'],
                'left_amount' => (float)$row['left_amount'],
                'left_amount_2' => (float)$row['left_amount_2'],
                'last_payment' => $row['last_payment'],
                'left_amount_in_currency' => $row['left_amount_in_currency'],
                'limit' => $lmt,
                'bonus' => $row['bonus'],
                'entry_amount' => (float)$row['entry_amount'],
                'entry_amount_2' => (float)$row['entry_amount_2'],
                'exit_amount' => (float)$row['exit_amount'],
                'exit_amount_2' => (float)$row['exit_amount_2'],
                'total_left' => (float)$row['entry_amount'] - (float)$row['exit_amount'],
            ];
            $sum_1 += (float)$row['entry_amount'];
            $sumInPeriod1 += (float)$row['entry_amount_2'];
            $sum_2 += (float)$row['exit_amount'];
            $sumInPeriod2 += (float)$row['exit_amount_2'];
            $sum_3 += (float)$row['entry_amount'] - (float)$row['exit_amount'];
            }
        }
        }
        return ['sums' => array('sales' => $sum_1,'payments' => $sum_2,'lefts' => $sum_3,'salesInPeriod' => $sumInPeriod1, 'paymentsInPeriod' => $sumInPeriod2),'customers' => $debts];
    }

    function sales_list($id = null,$keyword = null,$start_date = null,$end_date = null,$seller = null,$department = null,$user = null,$only_returns=null,$brand = null,$car_group = null,$cda = null){
        if (!$id && (!$start_date || !$end_date)) {return ['code' => Status_codes::HTTP_CONFLICT,'message' => 'Missed parameters','response' => []];}
    
        $query = $id ? " WHERE d._Document227_IDRRef = {$id}" : "";
        $query_condition = $keyword ? " AND (child_sales.code LIKE '%{$keyword}%' OR cust._Description LIKE '%{$keyword}%' OR prod._Fld1051 LIKE '%{$keyword}%') " : " ";
        $date_query = $start_date && $end_date ? " AND CONVERT(varchar, DATEADD(year, -2000, d2._Date_Time), 20) >= '{$start_date}' AND CONVERT(varchar, DATEADD(year, -2000, d2._Date_Time), 20) <= dateadd(day,1,'{$end_date}')"  : "";
        $seller_query = $seller ? " AND slr._Description = N'{$seller}' " : "";
        $dep_query = $department ? " AND dep._Description = N'{$department}' " : "";
        $brand_query = $brand ? " AND prod._Fld1034RRef = $brand " : "";
        $car_group_query = $car_group ? " AND prod._ParentIDRRef = $car_group " : "";
        $sql = "SELECT * FROM
                      (SELECT CONVERT(VARCHAR(1000), child_sales.sale_id, 1) as ID,CONVERT(VARCHAR(1000), child_sales.product_key, 1) as pro_ID,
                              child_sales.price,child_sales.first_price, child_sales.per,child_sales.no,child_sales.sale_date,
                              child_sales.code,CONVERT(VARCHAR(1000), child_sales.cust_id ,1) AS cust_ID, cust._Description as buyer,
                              child_sales.return_price,child_sales.return_code,child_sales.return_ID,child_sales.return_date,
                              prod_parent._Description as parent,prod_brand._Description as brand,prod._Fld1051 as brand_code,prod._Description as prod_name,
                              ISNULL(CAST((SELECT SUM(rtrn) FROM (SELECT CASE WHEN rby.sum IS NULL THEN d22._Fld5537 ELSE (d22._Fld5537 - rby.sum) END AS rtrn FROM _Document227_VT5532 d22
                                      OUTER APPLY (SELECT SUM(_Fld1953) AS sum FROM _Document131_VT1950 WHERE _Fld1952RRef = d22._Fld5540RRef AND _Fld1964_RRRef = d22._Document227_IDRRef) as rby
                                      LEFT JOIN _Document227 dp22 ON dp22._IDRRef = d22._Document227_IDRRef
                                      WHERE d22._Fld5540RRef = prod._IDRRef
                              AND CONVERT(varchar, DATEADD(year, -2000, dp22._Date_Time), 20) > DATEADD(year,-1,GETDATE())) as sale) AS int),0) as last_yc,
                              (SELECT SUM(rtrn) FROM (SELECT CASE WHEN rby.sum IS NULL THEN d22._Fld5537 ELSE (d22._Fld5537 - rby.sum) END AS rtrn FROM _Document227_VT5532 d22
                                      OUTER APPLY (SELECT SUM(_Fld1953) AS sum FROM _Document131_VT1950 WHERE _Fld1952RRef = d22._Fld5540RRef AND _Fld1964_RRRef = d22._Document227_IDRRef) as rby
                                      LEFT JOIN _Document227 dp22 ON dp22._IDRRef = d22._Document227_IDRRef
                                      WHERE d22._Fld5540RRef = prod._IDRRef
                              AND YEAR(CONVERT(varchar, DATEADD(year, -2000, dp22._Date_Time), 20)) = ".date('Y').") as sale) as ocy_1,
                              (SELECT SUM(rtrn) FROM (SELECT CASE WHEN rby.sum IS NULL THEN d22._Fld5537 ELSE (d22._Fld5537 - rby.sum) END AS rtrn FROM _Document227_VT5532 d22
                                      OUTER APPLY (SELECT SUM(_Fld1953) AS sum FROM _Document131_VT1950 WHERE _Fld1952RRef = d22._Fld5540RRef AND _Fld1964_RRRef = d22._Document227_IDRRef) as rby
                                      LEFT JOIN _Document227 dp22 ON dp22._IDRRef = d22._Document227_IDRRef
                                      WHERE d22._Fld5540RRef = prod._IDRRef
                                      AND YEAR(CONVERT(varchar, DATEADD(year, -2000, dp22._Date_Time), 20)) = ".(date('Y') - 1).") as sale) as ocy_2,
                              ISNULL((SELECT TOP 1 inf._Fld7653 as price FROM _Document248_VT6275 d248
                                          LEFT JOIN _Document248 dd ON dd._IDRRef = d248._Document248_IDRRef
                                          LEFT JOIN _InfoRg7647 inf ON inf._RecorderRRef = dd._IDRRef
                                          WHERE d248._Fld6277RRef = prod._IDRRef
                                          AND d248._Fld6283RRef = 0xA43568F728F0380D11E8D6CD6D31F204
                                          ORDER BY dd._Number DESC),0) as sale_price,
                              ISNULL((SELECT TOP 1 inf._Fld7653 as price FROM _Document248_VT6275 d248
                                          LEFT JOIN _Document248 dd ON dd._IDRRef = d248._Document248_IDRRef
                                          LEFT JOIN _InfoRg7647 inf ON inf._RecorderRRef = dd._IDRRef
                                          WHERE d248._Fld6277RRef = prod._IDRRef
                                          AND d248._Fld6283RRef = 0xA43568F728F0380D11E8D6CD6D31F207
                                          ORDER BY dd._Number DESC),0) as wholesale,
                              ISNULL((SELECT TOP 1 inf._Fld7653 as price FROM _Document248_VT6275 d248
                                          LEFT JOIN _Document248 dd ON dd._IDRRef = d248._Document248_IDRRef
                                          LEFT JOIN _InfoRg7647 inf ON inf._RecorderRRef = dd._IDRRef
                                          WHERE d248._Fld6277RRef = prod._IDRRef
                                          AND d248._Fld6283RRef = 0xA43568F728F0380D11E8D6CD6D31F206
                                          ORDER BY dd._Number DESC),0) as repairer_price,
                              qnt.numb as quantity,
                              (SELECT SUM(_Fld5046) FROM _Document218_VT5039 d WHERE d._Fld5041RRef = prod._IDRRef) as entry_count,
                              (SELECT SUM(_Fld5537) FROM _Document227_VT5532 d WHERE d._Fld5540RRef = prod._IDRRef) as out_count,
                              (SELECT SUM(_Fld1953) FROM _Document131_VT1950 d WHERE d._Fld1952RRef = prod._IDRRef) as out_2,
                              prod._Fld1022 as OEM,curr._Description as currency,
                              slr._Description as seller,dep._Description as department,
                              prod._Fld8982 as firm1,CAST(prod._Fld8984 AS INT) as count1,prod._Fld8983 as firm2,CAST(prod._Fld8985 AS INT) as count2,type
                       FROM (".($only_returns == 1 ? "" :
                         "(SELECT d._Fld5540RRef as product_key,
                              CONVERT(varchar, DATEADD(year, -2000, d2._Date_Time), 20) as sale_date,
                              d2._Number as code,d._Document227_IDRRef as sale_id,
                              d2._Fld5494RRef as cust_id,d2._Fld5489RRef as dep_id,
                              d2._Fld5496RRef as curr_id,d._Fld5556RRef as seller_id,
                              CAST(d._Fld5558 AS FLOAT) as first_price,
                              CAST(d._Fld5545 AS FLOAT) as price,
                              CAST(d._Fld5537 AS INT) as per,
                              rtt.rt_pr as return_price,rtt._Number as return_code,rtt.rt_ID as return_ID,rtt.date as return_date,
                              d._LineNo5533 as no,'sale' as type
                        FROM _Document227_VT5532 d
                        OUTER APPLY (SELECT TOP 1 dc._Fld1959 as rt_pr,dp._Number,CONVERT(VARCHAR(1000), dp._IDRRef,1) as rt_ID,
                                      CONVERT(varchar, DATEADD(year, -2000, dp._Date_Time), 20) as date
                                      FROM _Document131_VT1950 dc
                                      LEFT JOIN _Document131 dp ON dp._IDRRef = dc._Document131_IDRRef
                                      WHERE dc._Fld1964_RRRef = d._Document227_IDRRef AND  dc._Fld1952RRef = d._Fld5540RRef AND dp._Posted != 0x00) as rtt
                        LEFT JOIN _Document227 d2 ON d2._IDRRef = d._Document227_IDRRef
                        WHERE d2._Posted != 0x00 {$date_query})
                        UNION ALL")."
                        (SELECT d._Fld1952RRef as product_key,
                              CONVERT(varchar, DATEADD(year, -2000, d2._Date_Time), 20) as sale_date,
                              d2._Number as code,d._Document131_IDRRef as sale_id,
                              d2._Fld1932RRef as cust_id,d2._Fld1936RRef as dep_id,
                              d2._Fld1928RRef as curr_id,d._Fld1976RRef as seller_id,
                              NULL as first_price,CAST(d._Fld1959 AS FLOAT) as price,
                              CAST(d._Fld1953 AS INT) as per,
                              NULL as return_price,NULL as return_code,NULL as return_ID,NULL as return_date,
                              d._LineNo1951 as no,'return' as type
                        FROM _Document131_VT1950 d
                        LEFT JOIN _Document131 d2 ON d2._IDRRef = d._Document131_IDRRef
                        WHERE d2._Posted != 0x00 {$date_query})) as child_sales
                      LEFT JOIN _Reference77 prod ON prod._IDRRef = child_sales.product_key
                      OUTER APPLY (SELECT TOP 1 CAST(SUM(_Fld8510) AS INT) as numb
                                    FROM _AccumRgT8512
                                    WHERE _Fld8505RRef = child_sales.product_key
                                    AND _Fld8504RRef = 0x00000000000000000000000000000000
                                    AND YEAR(_Period) = '5999') as qnt
                      LEFT JOIN _Reference77 prod_parent ON prod_parent._IDRRef = prod._ParentIDRRef
                      LEFT JOIN _Reference78 prod_brand ON prod_brand._IDRRef = prod._Fld1034RRef
                      LEFT JOIN _Reference109 slr ON slr._IDRRef = child_sales.seller_id
                      LEFT JOIN _Reference69 cust ON cust._IDRRef = child_sales.cust_id
                      LEFT JOIN _Reference83 dep ON dep._IDRRef = child_sales.dep_id
                      LEFT JOIN _Reference35 curr ON curr._IDRRef = child_sales.curr_id
                    WHERE child_sales.no > 0 {$seller_query} {$dep_query} {$query_condition} {$brand_query} {$car_group_query}) as sales
                    ORDER BY sale_date,no";
    
        $sales = $this->remote_db->query($sql);
        $sales_last = [];
        $rows = $sales->result_array();
    
        $product_ids = array_unique(array_map(function($i) {return $i['pro_ID'];}, $rows));
    
        $orders_in_local_query = $this->local_db->query("SELECT oo.*, ord.department AS dep,ord.department_2 AS dep2,ord.date_time as ord_date,ord.date_time_2 as ord_date_2,
                                                                ord.count as order_count,ord.count_2 as order_count_2,CONCAT(u.name, ' ', u.surname) as user,CONCAT(u2.name, ' ',u2.surname) as user_2,
                                                                oo.`key` as prod
                                                          FROM `orders` oo
                                                          LEFT JOIN(SELECT CASE WHEN active = 1 THEN department ELSE '' END AS department,
                                                                            CASE WHEN active = 1 THEN date_time ELSE '' END AS date_time,
                                                                            CASE WHEN active = 1 THEN `count` ELSE '' END AS `count`,
                                                                            CASE WHEN active = 1 THEN user ELSE '' END AS user,
                                                                            CASE WHEN active_2 = 1 THEN department_2 ELSE '' END AS department_2,
                                                                            CASE WHEN active_2 = 1 THEN date_time_2 ELSE '' END AS date_time_2,
                                                                            CASE WHEN active_2 = 1 THEN count_2 ELSE '' END AS count_2,
                                                                            CASE WHEN active_2 = 1 THEN user_2 ELSE '' END AS user_2,`key`
                                                                    FROM orders_count oc
                                                                    WHERE oc.`key` IN ('".implode("','",$product_ids)."') AND `status` = 1
                                                                    ORDER BY date_time DESC LIMIT 1) ord
                                                          ON ord.`key` = oo.`key`
                                                          LEFT JOIN users u ON u.id = ord.user
                                                          LEFT JOIN users u2 ON u2.id = ord.user_2
                                                          WHERE oo.`key` IN ('".implode("','",$product_ids)."')");
        $orders_in_local = $orders_in_local_query->result_array();
        $new_orders = [];
        foreach ($orders_in_local as $sub_index => $order) {
          $new_orders = array_merge($new_orders,[$order['key'] => [
            "id" => $order["id"],
            "key" => $order["key"],
            "min" => $order["min"],
            "max" => $order["max"],
            "order" => $order["order"],
            "model" => $order["model"],
            "date_time" => $order["date_time"],
            "user" => $order["user"],
            "order_date" => $order["order_date"],
            "dep" => $order["dep"],
            "dep2" => $order["dep2"],
            "ord_date" => $order["ord_date"],
            "ord_date_2" => $order["ord_date_2"],
            "order_count" => $order["order_count"],
            "order_count_2" => $order["order_count_2"],
            "user_2" => $order["user_2"],
            "prod" => $order["prod"],
          ]]);
        }
    
        if ($user) {
          $mess_query = $this->local_db->query("SELECT `key` as prod FROM `message_view` WHERE `user_id` = {$user} AND `key` IN ('".implode("','",$product_ids)."') AND `status` = 0");
          $mess_read = $mess_query->result_array();
        }
        $msg_read_list = [];
        foreach ($mess_read as $mr) {
          $msg_read_list = array_merge($msg_read_list,[$mr->prod => 1]);
        }
    
        foreach ($rows as $key => $row) {
          if (!$cda || !$row['department'] || (is_bool($cda) && $cda) || (is_array($cda) && in_array($row['department'],$cda))) {
            $prod = $row['pro_ID'];
            $or = isset($new_orders[$prod]) ? $new_orders[$prod] : [];
    
    
            $unread = isset($msg_read_list[$prod]) ? $msg_read_list[$prod] : 0;
    
            $sales_last[] = [
              "ID" => $row['ID'],"pro_ID" => $row['pro_ID'],
              "no" => $row['no'],"sale_date" => $row['sale_date'],
              "code" => $row['code'],"buyer" => $row['buyer'],
              "brand" => $row['brand'],"brand_code" => $row['brand_code'],
              "prod_name" => $row['prod_name'],"per" => (int)$row['per'],
              "price" => $row['price'],
              "first_price" => $row['first_price'],
              "sale_price" => $row['sale_price'],"wholesale" => $row['wholesale'],
              "repairer_price" => $row['repairer_price'],"quantity" => (int)$row['quantity'],
              "entry_count" => (int)$row['entry_count'],"out_count" => (int)$row['out_count'],
              "out_2" => (int)$row['out_2'],"OEM" => $row['OEM'],
              // "canceled" => $row['canceled'],
              "return_price" => $row['return_price'],
              "return_code" => $row['return_code'],"return_ID" => $row['return_ID'],
              'return_date' => $row['return_date'],
              "currency" => $row['currency'],"seller" => $row['seller'],
              "department" => $row['department'],
              'cust_ID' => $row['cust_ID'],
              'min' => isset($or['min']) ? $or['min'] : '',
              'max' => isset($or['max']) ? $or['max'] : '',
              'order' => isset($or['order']) ? $or['order'] : '',
              'model' => isset($or['model']) ? $or['model'] : '',
              'in_cart' => isset($or['date_time']) ? $or['date_time'] : '',
              'ocy_1' => $row['ocy_1'] ? (int)$row['ocy_1'] : 0,
              'ocy_2' => $row['ocy_2'] ? (int)$row['ocy_2'] : 0,
              'last_yc_min' => $row['last_yc'] > 10 ? round($row['last_yc']/12) : ($row['last_yc'] > 5 ? round($row['last_yc']/6) : round($row['last_yc']/3)),
              'last_yc_max' => ($row['last_yc'] > 10 ? round($row['last_yc']/12)*3 : ($row['last_yc'] > 5 ? round($row['last_yc']/6)*2 : round($row['last_yc']/3)*2)),
              'unread' => $unread,
              'in_order' => (isset($or['dep']) && $or['dep']) || (isset($or['dep2']) && $or['dep2']) ? 1 : 0,
              'type' => $row['type'],
              'orders' => array(
                'department' => ($or && $row['firm1']) ? $row['firm1'] : '',
                'count' => ($or && $row['firm1']) ? $row['count1'] : '',
                'date' => (isset($or['ord_date']) && $row['firm1']) ? $or['ord_date'] : '',
                'user' => (isset($or['user']) && $row['firm1']) ? $or['user'] : '',
                'department_2' => ($or && $row['firm2']) ? $row['firm2'] : '',
                'count_2' => ($or && $row['firm2']) ? $row['count2'] : '',
                'date_2' => (isset($or['ord_date_2']) && $row['firm2']) ? $or['ord_date_2'] : '',
                'user_2' => (isset($or['user_2']) && $row['firm2']) ? $or['user_2'] : '',
              )
            ];
          }
        }
        return $sales_last;
      }
    }