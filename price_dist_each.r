#setwd('C:/Xiwei/projects/member_price_profiling')

#This file is used for plotting the price distribution for each sortkey.
#We do each key individually mostly because the total transaction data become too large to export when we have >2k keys


transprice = read.table('transaction_prices.txt', header=T,  stringsAsFactors =F)  #All transaction prices
sortkey = unique(transprice$sortkey) #Identify the sortkey
sortkeyinfo = read.csv('sortkey_info.csv', stringsAsFactors =F)
avgksn = read.table('average_prices.txt', header=T,  stringsAsFactors =F)  #Average KSN price data

prices = transprice$TRS_PRICE
avgprice = mean(prices)
sdprice = sd(prices)
prices[prices > (avgprice+3*sdprice)]= avgprice+3*sdprice  #remove outliers
prices[prices < (avgprice-3*sdprice)]= avgprice-3*sdprice  #remove outliers
 
##Make histogram of all the transaction prices
soarnm = sortkeyinfo$SOAR_NM[sortkeyinfo$sortkey == sortkey ]  #identify soarnm/subctg desc of each sortkey
subctg = sortkeyinfo$sub_catg_desc[sortkeyinfo$sortkey == sortkey ]
#png(file=paste(soarnm,subctg,sortkey,".png"))
pdf(file=paste(soarnm,subctg,sortkey,".pdf"))
par(mfrow=c(2,1),oma=c(0,0,3,0))
hist(prices,breaks=20,xlab='All Transaction Prices')
  
##Find 33th 66th quantile, draw the cutoff line
fn = ecdf(prices)
x=seq(0,max(prices),0.1);
y=100.0*sapply(x,fn);
cutpointy = c(33,66)
cutpointx_alltrs=approx(y,x,cutpointy)$y;
abline(v= cutpointx_alltrs, col='red', )

##Make histogram of the avg ksn prices
prices = avgksn$Average.TRS_PRICE.
avgprice = mean(prices)
sdprice = sd(prices)
prices[prices > (avgprice+3*sdprice)]= avgprice+3*sdprice
prices[prices < (avgprice-3*sdprice)]= avgprice-3*sdprice
hist(prices,breaks=20, xlab = 'Average KSN Prices')

##Find 33th 66th quantile, draw the cutoff line
fn = ecdf(prices)
x=seq(0,max(prices),0.1);
y=100.0*sapply(x,fn);
cutpointy = c(33,66)
cutpointx_alltrs=approx(y,x,cutpointy)$y;
abline(v= cutpointx_alltrs, col='red', )
mtext(text = paste(sortkey,soarnm,subctg), line=20, font=2, adj=1, cex=1.2 )

dev.off()