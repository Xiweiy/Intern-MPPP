MPPP.sh -- The overall code to 
	1. select the sortkeys based on the following criteria 
		(1)contain >=3 ksn_id, 
		(2) have >100k transactions. 
	2. Rank the ksn_ids within each sortkey, and 
	3. label each member's price preference in diff. BU.
 
output_trs.sh -- The code for output (1) all transaction prices of all ksn_id, (2) average transaction prices of each ksn_id, for plotting the price distribution for one sortkey only. 
	We only do one sortkey each time because of the huge size of the all transaction prices. 
	Output two files, 'transaction_prices.txt' and 'average_prices.txt', which are later used by the r script 'price_dist_each.r'. 

price_dist_each.r -- Read in the data file output by output_trs.sh, and draw the price distribution for each sortkey. Each price distribution plot is in pdf format with name as soarnm+subctg_desc+sortkey. 
	Also require file 'sortkey_info.csv' for adding the graph title.

MemberPricePreference.pdf -- The presentation slides as an introduction for this project.

MemberPricePreference.xlsx -- Sample Result. Include the list of selected sortkey, and part of the sample data output by MPPP.sh.

sortkey_info.csv -- The list of selected sortkey, including info like soar_nm, fp_unit_desc, fp_dvsn_desc, fp_dept_desc, fp_catg_grp_desc, catg_desc, sub_catg_desc, sortkey, nKSNID, nTRS, revenue for each key. Used by 'price_dist_each.r'.