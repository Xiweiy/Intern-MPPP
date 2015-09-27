#!/bin/bash

# This script up loads the Twitter data the Teradata DB.
#
# Written by: Xiwei Yan
# Date: 2015-07-20

tStart=`date`
echo $0 started at $tStart.


TERADATA="1700"
USER_PWD="xyan0,N3pf79e5yt"
SERVER="TDAdhoc.intra.searshc.com"
PERM_DB="L2_MRKTGANLYS_T"


bteq <<EOF > bteq.log
.LOGON ${SERVER}/${USER_PWD};
.SET WIDTH 1024

--------------------------------------------------------------------------------------------------------
------Step #1 Select the Subcategories that have >=3 ksn_id and >100k TRANSACTIONS (PAST YEAR)
-------------------------------------------------------------------------------------------------------

DROP TABLE SHC_WORK_TBLS.D5_XY_SORTKEY;
CREATE TABLE  SHC_WORK_TBLS.D5_XY_SORTKEY AS (
	SELECT 
		soar_nm, 
		FP_UNIT_DESC,
	  	FP_DVSN_DESC ,
	   	FP_DEPT_DESC,
	   	FP_CATG_GRP_DESC,
	    CATG_DESc,
	    sub_catg_desc,
	   CAST(
			(CASE WHEN FP_UNIT_NBR IS NULL THEN '99'  ELSE CAST(CAST(FP_UNIT_NBR AS FORMAT'9(2)') AS CHAR(2))  END
			||'-'||CASE WHEN FP_DVSN_NBR IS NULL THEN '999'  ELSE CAST(CAST(FP_DVSN_NBR AS FORMAT'9(3)') AS CHAR(3))  END
			||'-'||CASE WHEN FP_DEPT_NBR IS NULL THEN '9999'  ELSE CAST(CAST(FP_DEPT_NBR AS FORMAT'9(4)') AS CHAR(4))  END
			||'-'||CASE WHEN FP_CATG_GRP_NBR IS NULL THEN '9999'  ELSE CAST(CAST(FP_CATG_GRP_NBR AS FORMAT'9(4)') AS CHAR(4))  END
			||'-'||CASE WHEN CATG_NBR IS NULL THEN '9999' ELSE CAST(CAST(CATG_NBR AS FORMAT'9(4)') AS CHAR(4)) END
			||'-'||CASE WHEN sub_catg_nbr IS NULL THEN '99' ELSE CAST(CAST(sub_catg_nbr AS FORMAT'9(2)') AS CHAR(2)) END
			) AS CHAR(24) ) AS sortkey,
	   count(distinct A.ksn_id) AS nKSNID,
	   sum(nKSN_TRS) AS nTRS,  
	   SUM(KSN_Revenue) AS Revenue
	 FROM cbr_mart_tbls.rd_kmt_soar_bu A
	 INNER JOIN (
	 	SELECT 
	 		KSN_ID,
			COUNT(DISTINCT CUS_IAN_ID_NO) AS nKSN_TRS,
			SUM(kmt_sell) AS KSN_Revenue
	 	FROM CRM_PERM_TBLS.SYWR_KMT_SALES_PTD 
		WHERE DAY_DT BETWEEN '2014-07-01' AND '2015-06-30'
			AND SELLQTY >0
	 	GROUP BY 1
	 	) B    ---group by ksn_id first because there is not enough spool space for joining the two table directly for records in the entire year
 	ON A.KSN_ID = B.KSN_ID
	GROUP BY 1,2,3,4,5,6,7,8 --group by Sort-Key
	HAVING nKSNID >=3
		AND nTRS >100000  -- TRANSACTION > 100k
	--ORDER BY soar_nm
) WITH DATA PRIMARY INDEX(sortkey);
COLLECT STATS  SHC_WORK_TBLS.D5_XY_SORTKEY INDEX(sortkey);


----------------------------------------------------------------------------------------------------
-------STEP 2 For these Sub_Categories, Find out the price level of the KSN_IDs in this Sub_Category
---------------------------------------------------------------------------------------------------
DROP TABLE SHC_WORK_TBLS.D5_XY_RANKS;
CREATE TABLE SHC_WORK_TBLS.D5_XY_RANKS AS (
	SELECT 
		A.KSN_ID,
        CAST(
				(CASE WHEN FP_UNIT_NBR IS NULL THEN '99'  ELSE CAST(CAST(FP_UNIT_NBR AS FORMAT'9(2)') AS CHAR(2))  END
			||'-'||CASE WHEN FP_DVSN_NBR IS NULL THEN '999'  ELSE CAST(CAST(FP_DVSN_NBR AS FORMAT'9(3)') AS CHAR(3))  END
			||'-'||CASE WHEN FP_DEPT_NBR IS NULL THEN '9999'  ELSE CAST(CAST(FP_DEPT_NBR AS FORMAT'9(4)') AS CHAR(4))  END
			||'-'||CASE WHEN FP_CATG_GRP_NBR IS NULL THEN '9999'  ELSE CAST(CAST(FP_CATG_GRP_NBR AS FORMAT'9(4)') AS CHAR(4))  END
			||'-'||CASE WHEN CATG_NBR IS NULL THEN '9999' ELSE CAST(CAST(CATG_NBR AS FORMAT'9(4)') AS CHAR(4)) END
			||'-'||CASE WHEN sub_catg_nbr IS NULL THEN '99' ELSE CAST(CAST(sub_catg_nbr AS FORMAT'9(2)') AS CHAR(2)) END
			) AS CHAR(24) ) AS sortkey,
        A.soar_nm,
        A.FP_UNIT_DESC,
        A.sub_catg_desc,
		AVG(kmt_sell/SELLQTY) AS AVG_TRS_PRICE,
		STDDEV_POP(kmt_sell/SELLQTY) AS STD_DEV,
		(CASE WHEN (AVG_TRS_PRICE - 3*STD_DEV) <0 THEN 0 ELSE (AVG_TRS_PRICE - 3*STD_DEV) END) AS LOWER_BOUND,
		(AVG_TRS_PRICE + 3*STD_DEV) AS UPPER_BOUND,
		CAST(((RANK() OVER (PARTITION BY SORTKEY ORDER BY AVG_TRS_PRICE DESC)-1)*2.99)/(COUNT(*) OVER(PARTITION BY SORTKEY ORDER BY AVG_TRS_PRICE))+1 AS INT) AS PRICE_LEVEL
	FROM  cbr_mart_tbls.rd_kmt_soar_bu A
	INNER JOIN CRM_PERM_TBLS.SYWR_KMT_SALES_PTD B
		ON A.KSN_ID = B.KSN_ID	
	WHERE sortkey IN (SELECT SORTKEY FROM SHC_WORK_TBLS.D5_XY_SORTKEY)
		AND DAY_DT BETWEEN '2014-07-01' AND '2015-06-30'
		AND SELLQTY >0
	GROUP BY 1,2,3,4,5
) WITH DATA PRIMARY INDEX(KSN_ID);
COLLECT STATS SHC_WORK_TBLS.D5_XY_RANKS INDEX(KSN_ID);



----------------------------------------------------------------------------------------------------
-------STEP 3 Group by Member-BU to find out the price preference of each member across diff. BU
--------------------------------------------------------------------------------------------------
DROP TABLE SHC_WORK_TBLS.D5_XY_MEMBERS;
CREATE TABLE SHC_WORK_TBLS.D5_XY_MEMBERS AS ( 
	SELECT
		A.lyl_id_no,
		FP_UNIT_DESC,
		COUNT(DISTINCT A.CUS_IAN_ID_NO) AS nTRS,
		COUNT(DISTINCT A.KSN_ID) AS nITEMS,
		SUM(CASE WHEN PRICE_LEVEL=1 THEN SELLQTY ELSE 0 END) AS nHIGH,
		SUM(CASE WHEN PRICE_LEVEL=2 THEN SELLQTY ELSE 0 END) AS nMEDIUM,
		SUM(CASE WHEN PRICE_LEVEL=3 THEN SELLQTY ELSE 0 END) AS nLOW,
		SUM(kmt_sell) AS Spending
	FROM CRM_PERM_TBLS.SYWR_KMT_SALES_PTD A
	INNER JOIN SHC_WORK_TBLS.D5_XY_RANKS B
		ON A.KSN_ID = B.KSN_ID
	WHERE DAY_DT BETWEEN '2014-07-01' AND '2015-06-30'
		AND SELLQTY >0
		AND kmt_sell/SELLQTY > LOWER_BOUND   --disregard the outliers 
		AND kmt_sell/SELLQTY < UPPER_BOUND
		AND lyl_id_no IS NOT NULL
	GROUP BY 1,2
	HAVING nTRS>5
--	ORDER BY lyl_id_no, FP_UNIT_DESC
) WITH DATA PRIMARY INDEX(lyl_id_no);
COLLECT STATS  SHC_WORK_TBLS.D5_XY_MEMBERS INDEX(lyl_id_no);

.LOGOFF;
EOF

	RC=$?
	echo bteq RC: $RC
	if [ $RC -gt 10 ];
	then
		echo $0 failed.  Return Code: $RC
		exit 1
	else
		echo $0 completed.
	fi
	#
done


tEnd=`date`
echo $0 started at $tStart.
echo $0 ended at $tEnd.

#
# The End
