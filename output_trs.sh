#!/bin/bash

# This script up loads the Twitter data the Teradata DB.
#
# Written by: Xiwei Yan
# Date: 2015-07-12

tStart=`date`
echo $0 started at $tStart.


TERADATA="1700"
USER_PWD="xyan0,N3pf79e5yt"
SERVER="TDAdhoc.intra.searshc.com"
PERM_DB="L2_MRKTGANLYS_T"

for key in `awk '{print $1}' sortkey.txt`
do
	CLASS_KEY=${key//\"/}
	echo $CLASS_KEY	

	rm -f transaction_prices.txt
	rm -f average_prices.txt

	bteq <<EOF > bteq.log
	.LOGON ${SERVER}/${USER_PWD};
	.SET WIDTH 1024

	-------------------------------------------------------------------------
	-------STEP 1 SELECT ALL TRANSACTION RECORDS IN ONE SUBCATEGORY & Export All transaction prices
	-------------------------------------------------------------------------
	DROP TABLE SHC_WORK_TBLS.D5_XY_TRSDATA;
	CREATE TABLE SHC_WORK_TBLS.D5_XY_TRSDATA AS (
		SELECT 
			A.KSN_ID,
			A.CUS_IAN_ID_NO,
			--B.SOAR_NM,
			--B.sub_catg_DESC,
			CAST(
				(CASE WHEN FP_UNIT_NBR IS NULL THEN '99'  ELSE CAST(CAST(FP_UNIT_NBR AS FORMAT'9(2)') AS CHAR(2))  END
				||'-'||CASE WHEN FP_DVSN_NBR IS NULL THEN '999'  ELSE CAST(CAST(FP_DVSN_NBR AS FORMAT'9(3)') AS CHAR(3))  END
				||'-'||CASE WHEN FP_DEPT_NBR IS NULL THEN '9999'  ELSE CAST(CAST(FP_DEPT_NBR AS FORMAT'9(4)') AS CHAR(4))  END
				||'-'||CASE WHEN FP_CATG_GRP_NBR IS NULL THEN '9999'  ELSE CAST(CAST(FP_CATG_GRP_NBR AS FORMAT'9(4)') AS CHAR(4))  END
				||'-'||CASE WHEN CATG_NBR IS NULL THEN '9999' ELSE CAST(CAST(CATG_NBR AS FORMAT'9(4)') AS CHAR(4)) END
				||'-'||CASE WHEN sub_catg_nbr IS NULL THEN '99' ELSE CAST(CAST(sub_catg_nbr AS FORMAT'9(2)') AS CHAR(2)) END
				) AS CHAR(24) ) AS sortkey,
			kmt_sell/SELLQTY AS TRS_PRICE
		FROM  CRM_PERM_TBLS.SYWR_KMT_SALES_PTD A
		INNER JOIN cbr_mart_tbls.rd_kmt_soar_bu B
			ON A.KSN_ID = B.KSN_ID
		WHERE DAY_DT BETWEEN '2015-01-01' AND '2015-06-01'
			AND SELLQTY >0
			--AND sortkey = '${CLASS_KEY}'
			AND sortkey ='03-035-0715-2586-0088-03'
	) WITH DATA PRIMARY INDEX(CUS_IAN_ID_NO);
	COLLECT STATS SHC_WORK_TBLS.D5_XY_TRSDATA INDEX(CUS_IAN_ID_NO);


		.EXPORT RESET
		.EXPORT FILE transaction_prices.txt

		SELECT * FROM SHC_WORK_TBLS.D5_XY_TRSDATA;

	-------------------------------------------------------------------------
	-------STEP 2 SELECT AVG TRANSACTION PRICE FOR EACH KSNID IN ONE SUBCATEGORY & output
	-------------------------------------------------------------------------
		.EXPORT RESET
		.EXPORT FILE average_prices.txt

		SELECT 
			KSN_ID,
			sortkey,
			AVG(TRS_PRICE)
		FROM SHC_WORK_TBLS.D5_XY_TRSDATA
		GROUP BY 1,2;

		.LOGOFF;

EOF

	RC=$?
	echo bteq RC: $RC
	if [ $RC -gt 10 ];
	then
		echo $0 failed.  Return Code: $RC
		exit 1
	else
		sed '2d' transaction_prices.txt > tmp.txt; mv tmp.txt transaction_prices.txt 
		sed '2d' average_prices.txt > tmp.txt; mv tmp.txt average_prices.txt 
	fi
	#

	R CMD BATCH --no-save --no-restore price_dist_each.r

done


tEnd=`date`
echo $0 started at $tStart.
echo $0 ended at $tEnd.

#
# The End
