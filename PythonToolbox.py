,# -*- coding: utf-8 -*-
"""
Created on Sat Jul 28 11:47:12 2018

@author: wudi
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jul 02 09:29:39 2018

@author: wudi
"""

#Read CSV
import pandas as pd
df=pd.read_csv('U:/CSPort/Crosslist.csv')

#head/tail
df.head()
df.tail()

#Select Column
df['GURUticker'].head()
#Replace string in dataframe
crosslist['GURUticker']=crosslist['GURUticker'].str.replace(':','_')
#Show Colunm names
list(Crosslist)

#show column number/row number/dimension
#nrow/ncol
len(Crosslist.columns)
len(Crosslist.index)
Crosslist.shape

#Define new dataframe
dfgen=pd.DataFrame()
#Initialize a dataframe with 0 and fixed number of row counts 
target_item=pd.DataFrame(0,index=range(len(target_item.index)),columns=range(1))
#Define with column names
signals=['ASIG','SSIG','VSIG','QSIG']
signpalerf=pd.DataFrame(columns=signals)

a.columns=['abc','def']

#Get the date series/convert to list first
days=pd.date_range(start='2007-01-01',end='2018-5-31',freq='D').tolist()
dfgen['days']=pd.DataFrame(days)

#define & fill a list in list
items=[[]for i in range(len(quarterly))]
for n in range(3,len(quarterly),1):
    items[n-2].append((list(document['financials']['quarterly'][quarterly[n]])))
#loop
for n in range (1,10,1):
    s=n+1

#Define dictionary of listt/ can use string to name list, while list of list cannot use string as naming convention
from collections import defaultdict
dict=defaultdict(list)
for t in range(3,len(quarterly),1):
       dict[quarterly[t]].append((list(document['financials']['quarterly'][quarterly[t]])))
       
#Construct filepath
Date_Sales=pd.read_csv(os.path.join('U:/CSPort/Date_Sales/',(BBGT+'_Date_Sales.csv')))    

#Conditional selection
BBGT=Crosslist.ix[(Crosslist['BBGticker']==name)]['BBGticker'][0]

#Change column orders by name
Date_Sales=Date_Sales[['date','Rev','Announcement_DT']]

#Order column of oen dataframe, follow another dataframe, assuming names are the same
returntab=returntab[df_z.columns]

#Show first column of Dataframe 
dfreshaped.ix[:,1].head()

#Cbind columns of dataframe
import numpy as np
df_z=pd.concat([self.df.iloc[:,0].reset_index(drop=True),zstab],axis=1)                         #combine the date column to zs table

#Merge, left outer join
dfnew=pd.merge(dfnew,Guru_newitem,on='days',how='left')    

#Checkk Python version
import system as sys
sys.version

#Get the unique elemetn of the list
myset=set(mylist)
mynewlist=list(myset)

#Drop duplicate rows (unique) in dataframe
cosntgeneral=cosntgeneral.drop_duplicates(keep='first',inplace=False)

#if NA, fill with the preceding value
dfnew[name]=dfnew[name].fillna(method='ffill')

#Fill NA with 0
df=df.fillna(0)

#Paste string to every elements in the list
string=[x+' Equity' for x in string]

#Sum products of two columns
Fact_Perf=sum(dfnew['weights']*dfnew['value'].fillna(0)/100)

#Get yesterday's date as 20080303
yesterday=date.today()-timedelta(1)
dat=yesterday.strftime('%Y%m%d')

#Change column name
Pos=Pos.rename(columns={'ticker':'BBG'})

#Convert Percentage string to float
Pos['NAV']=Pos['NAV'].str.rstrip('%').astype('float')/100

#Rename all columns
Gururev.columns=['Rev','target_item']

#Download the day's data from BBG
BBG=list(df['BBG'])
Daily_Return=con.ref(BBG,["CHG_PCT_1D"])
    
#Drop duplicated items
df5=df5.drop_duplicates(keep=False)     #remove duplicated rows
a=a.drop_duplicates(keep="first")


#Generate dates
dfnew=pd.DataFrame(pd.date_range(start='2007-01-01',end='2018-5-31',freq='D').tolist())

#Assigne value to a specific column/row
df5.loc[0,'InfoPublDate']='2007-01-01'

#Convert dataframe of strings into list
df=pd.read_csv('U:/AQuant/Universe.csv')
tickerlist=df['Ticker'].tolist()
tickerlist=[str(x) for x in tickerlist]
tickerlist=[str(x).zfill(6) for x in tickerlist]

#Put a list in Python SQL Query
queryString="""
      SELECT CompanyCode FROM JYDBBAK.dbo.SecuMain WHERE (SecuCode IN("""+""", """.join((n for n in tickerlist))+""")) AND (SecuCategory=1)
    """
reslist5=ms.ExecNonQuery(queryString)

#Get title from SQLquery's return
title=[i[0]for i in reslist5.description] #Get the titles from SQL Query
df5.columns=title

#Reshape /Pivot the datatable
a=df5.pivot_table(index='InfoPublDate',columns='SecuCode',values='ROE',aggfunc='first')

#save as csv
dfnew.to_csv("U:/MISC/test.csv",index=False)

#convert dataframe column into datetime
InfoDay=pd.to_datetime(df_z['InfoPublDate'])

#Return days in week given datetime
dayinweek=InfoDay.dt.weekday

#grouping as decil across rows
grouping=(c.transpose().rank(method='first').apply(lambda x: pd.qcut(x, 10, labels=list(range(10))), axis=0)).transpose() #transpose, do the qcut on column, and transpoe back (same as doing qcut across rows)

#grouping as decil in one column
grouping=pd.qcut(df['a'],10,labels=False,retbins=True)

#grouping as applied to each column
grouping=df.apply(lambda x: pd.qcut(x, 5, labels=list(range(5))), axis=0)

#copy dataframe
#df1=df2 is wrong, it should be:
df1=df2.copy()

#Assign value to the whole table, conditional on value of a column
dfzcol=self.df.columns #take the column names
df_rebel.loc[df_rebel['dayinweek']<>wd,dfzcol]=np.NaN #Assign value to columns included in dfzcol, conditional on value in column dayinweek

#string replacement
timetable['Starttime']=starttime.replace("'","")
univ=[str.replace(x,"'","")+" CH Equity" for x in self.univ]
gics['Tickers']=[str.replace(x," CH Equity","") for x in gics['Tickers']]
 
#apply to a list of strings/ string replacement
newtickers=[str.replace(x,"CH Equity"," CH") for x in tickers]
tickers=[(x + " CH")for x in tickers]

#Insert to a list, at the beginning
tickers.insert(0,'InfoPublDate')

#Apply function to every column without really using apply
meanreturn8=[returntab_T.loc[grouping_T[x]==8,x].mean() for x in range(0,1275)]

#Python number counts:
for i in range (0:9): print (i) #will just print 0 to 8, 9 is not included;
for i in range(9): print(i)# will print 9

#Subset dataframe according to value in a column
subset=df.loc[(df['InfoPublDate']>=starttime)&(df['InfoPublDate']<=endtime),]

#subset dataframe according to column of A ==as column of B
returntab=returntab[returntab['InfoPublDate'].isin(df['InfoPublDate'])]           

#subset dataframe by column names
df1 = df[['a','b']]

#convert the dataframe into float format
dfpivot2=dfpivot2.astype(float)

#change column name by position
subset.columns.values[len(subset.columns)-1]='toptile'

#subset conditional on value in a column 
subset=subset[subset['BMReturn']!=0]

#drop duplicates, and keep unique in the list
signallist=list(set(signallist))
datelist=list(CSI300memb['date'].unique())

#Get a list of file in the folder
filelist=os.listdir()

#String capture
signal=signals.split("_",2)[0]+'_'+signals.split("_",2)[1]

#Select the overlap columns froma big table
returntab=returntab[df.columns]

#Apply numpy-oriented function to dataframe, and convert it back to dataframe
rsitab=[ta.RSI(np.array(RSIyear.iloc[:,x]))for x in range(len(RSIyear.columns))]
RSIyear=pd.DataFrame(dict(zip(univ,rsitab)),columns=univ)

#select multiple columns by name
zs=sigzs[['InfoPublDate',ticker]]

#rbind
returntab=returntab.append(self.yearreturn(signal,indx,year,ngroup),ignore_index=True)

#Difference of two dataframes
csi500=pd.concat([csi800,csi300]).drop_duplicates(keep=False)

#Sort value in dataframe
df=df.sort_values(by='col1', ascending=False)

#remove/delete row based on number
df=df[df[title].values!='600011']

#Drop columns of NaN
df_rebel=df_rebel.dropna(axis=1,how='all')

#Drop rows if any of the column is NA
df_rebel=df_rebel.dropna()

#intersection of list
t=list(set(t) & set(templist))

#if column exists
if ticker in sigzs.columns:
    
#define dataframe with column names given
zs=pd.DataFrame(0,index=range(len(sigzs.index)),columns=[ticker])

#Numpy fill nan with 0
x_train[np.isnan(x_train)]=0

#Select top 100 columns value of each row /column names as content
top100=pd.DataFrame(0,index=range(0),columns=colnames)
 for i in x.columns:
        df1row=pd.DataFrame(x.nlargest(100,i).index.tolist(),index=colnames).T
        top100=pd.concat([top100,df1row],axis=0)
        
#Reverse of pivot, convert columns names as column value
single_signal_peryear=pd.melt(GICSzs,id_vars='InfoPublDate',value_vars=list(GICSzs.columns[1:len(GICSzs.columns)-1]),var_name='Ticker',value_name='ZS')    
melting=pd.melt(funda,id_vars=['publdate','enddate','ticker'],value_vars=list(funda.columns[3:]),var_name='signame',value_name='sigvalue')

#Turn table upside down
returntab2.iloc[::-1]

#Calculate forward rolling return
returntab3=returntab2.iloc[::-1].rolling(min_periods=1,window=30).sum()[::-1]

#countif in pandas on column
df[df.a > 1].sum()   
df['count'] = (df[cols].values=='s').sum(axis=1) #countif on rows 

#Initialize new array
a=np.array([0,0,0,0,3,10,5,7,9,8])

#sort column according to values in row
rebel_mkt_perc.iloc[:,1:]=rebel_mkt_perc[rebel_mkt_perc.iloc[-1,1:].sort_values().index]

#construct date with integer
rebel_day=str(datetime(year, i, 1).date())

#Compare the two list, get the difference:
a=set(CSI800)-set(CSI300)   #CSI800 is the bigger list, covering the smaller list CSI300

#Pivot table
a=dfo.groupby(['Index','Year','GICS_SECTOR'],as_index=False).agg({"Weight":"sum"})
ranktab=ranktab.groupby(['ticker'])['rank'].sum()
#Numpy Opeartions
a=np.random.random((10, 10))        #Create a numpy matrix with random number
b=stats.zscore(a,axis=1,ddof=1)     #Turn the numpy array into zscore
zstab=pd.DataFrame(b)               #Turn the numpy array into matrix
row_idx=np.array([0,1,3])          
col_idx=np.array([0,2])
a_2=a[row_idx[:,None],col_idx]      #INdexing the numpy array [in numpy, the dataframe row is treated as one array]
np.vstack((zs_sec3,[zs_sec3[i]]*freq[i])).transpose()  #vstack is used to append array to the array matrix
train_results.append(roc_auc)                          #Append is used to append number to an array 

#Check numpy version
print(np.__version__)


#Mask operation
ZS=np.full_like(fullZStab1,np.nan)
fullZStab1=np.array(fullmetrictab1.iloc[:,1:].values,dtype=np.float)
col_idx=np.where(np.isin(fullmetrictab1.columns,yearticker))[0]     #Choose the column according to name
row_idx=np.where((fullmetrictab1['InfoPublDate']>=starttime)&(fullmetrictab1['InfoPublDate']<=endtime))[0]  #Choose the row according to year
maskarray=np.full_like(fullZStab1,np.nan)         #a masked model with nan
maskarray[row_idx[:,None],(col_idx-1)]=1          #fill the useful one with 1,others nan
mask=np.isnan(fullZStab1)|np.isnan(maskarray)     #a mask chosen from nan 
mdata=np.ma.masked_array(fullZStab1,mask)         #mdata is masked array with raw data above and masked label below, onnly apply to the False one (non-maksed one)
zscore=pd.DataFrame(mstats.zscore(mdata,axis=1,ddof=1).filled(np.nan))

#Choose isn't in
col_idx_inv=np.where(~np.isin(signaldf.columns[1:],yeartickerlst))[0]

#Get the percentile of the numpy array (Ndarray)
np.nanpercentile(a, 50, axis=1, keepdims=True)
quintiles = np.nanpercentile(Sec_Groupnp, [20,40,60,80],axis=1,keepdims=True)


#Grouping the numbers with bins
bins=np.array([-3,-2,-1,0,1,2,3])
Sec_Groupnp[row_idx[:,None],]=np.digitize(Sec_Groupnp[row_idx[:,None],],bins,right=False)

#Pick out the index of nan in an numpy array
nanidxnp=np.argwhere(np.isnan(Sec_ZSnp))

#Initialize two-d array
a=np.array([1,3,5])
b=np.array([5,6,2])
c=np.vstack((a,b))


x = np.array([[ 0,  1,  2],
...            [ 3,  4,  5],
...            [ 6,  7,  8],
...            [ 9, 10, 11]])

y = np.arange(35).reshape(5,7)  

#nanmean, conditional on another array's value 
m=[np.nanmean(returnnp[Sec_Groupnp==val],axis=1)for val in range(0,4)]

#mask the ndarray by keeping the same dimension
np.where(Sec_Groupnp==val,returnnp,np.nan)
returnnp[SecGroup==val] #Won't work, this keeps the selected elements only, not the dimension 

#Numpy ndarray indexing
y = np.arange(35).reshape(5,7)
y[np.array([0,2,4]),1]  #Get the column 1 o row 0,2,4 out

#Check if file exist
if os.path.isfile(filepath)==False:
    
#Check if substring is in string
if "/" in "ABC/DEF": print('yay')

#Cut the string before and after a character:
signal1=signal.split("/")[0]
signal2=signal.split("/")[1]

#conditional replacement 
year_tix.loc[year_tix['GICS_SECTOR']==sec,'GICS_SECTOR']=100

#Initialize an array with shape
 np.zeros((2, 1))
 
#Given the element, the location of the element in list
signallist.index(signal)

#convert a pandas index to column
df['index'] = df.index
df.reset_index(inplace=True) #(to first columsn)

#Transpose dataframe and keep the first column as headers 
at=a.set_index('InfoPublDate').T

#Conditional replace the value in dataframe
df_rebel[df_rebel.loc[(df_rebel['dayinweek']==1) ,df_rebel.columns!='InfoPublDate']>9]=np.nan

#convert dataframe columns into dict
mydict=dict(zip(alphadf2['Bucket'],alphadf2['JYDBMain']))

#fill 0 with precedent number
RSI=RSI.replace(to_replace=0,method='ffill')

#remove column by name
df = df.drop('column_name', 1)

#return the difference of two rows
a.loc[(a['month']!=3),(a.columns!='EndDate')&(a.columns!='month')]=a.loc[:,(a.columns!='EndDate')&(a.columns!='month')]-a.loc[:,(a.columns!='EndDate')&(a.columns!='month')].shift(1)

#Subset the dataframe according to the nsmallest of a column 
smallest=general.nsmallest(30,'mean')

#sumproduct in python 
weightedmean=(dfnew['weight']*dfnew['PNL']).sum()

#replace na with value in another column
memb.Quandl.fillna(memb.Ticker, inplace=True)

#Use Dictionary to save a bunch of dataframes
datadict={}
for quandltic in combinedtix:
    datadict[quandltic]=sec_ind.copy()
#excluding columns using LOC
growth=indictab.loc[indictab['month']==12,~indictab.columns.isin(['calendardate','month'])]

#replace all inf as nan
df=df.replace(np.inf,np.nan)

#concatenate to every string in the list
newcolnames=[quandltic+indicator for indicator in indicators]

#add a column to the first place of the dataframe
dfreshaped.insert(0,column='date',value=dfreshaped.index)

#insert column to the first column
df.insert(loc=0, column='A',value=new_col)

#select multiple columns based on values
a.loc[:,a.iloc[0,:]>0.1]

#get the common elements of lists
common_tickers=set(shortlisted_stock[indicators[0]])
for indic in indicators:
                common_tickers.intersection_update(shortlisted_stock[indic])
            
#Get the row index of first occurence, based on conditions
max(df[df['ticker']=='NVDA'].index.values)

#Choose the strings conditional in list
growth_indics=[s for s in indicators if 'Growth' in s]


#compound cumulative return
returntab['CumReturn']=(1+returntab['Avg_Return']).cumprod() - 1 

#return the rank of a column value
mcap.loc[:,'rank']=mcap['marketcap'].rank(ascending=True)

#initialize dataframe with column names
pd.DataFrame(columns=['date','Gp1','Gp2','Gp3','Gp4','Gp5'])

#drop rows depending on duplicated value in column
mcap=mcap.drop_duplicates(subset='ticker')

#Add a string to list
a=['date']+indicators

#Insert a column to a location of a dataframe
df2.insert(loc=0,column='date',value=dfreshaped['date'])

#Initialize a Dataframe with colum-name defined:
pnl_tab=pd.DataFrame(sectorlist,columns=['sector'])

#Add leading 0 zero
master['abc'].str.zfill(7)

#Count how many fullfilled the criteria in a row
df_ptier['nhigh']=(df_ptier[cols]=='low').sum(axis=1)

#Combine the strings in two dataframe
df_category=df_ptier.iloc[:,1:].astype(str)+dfrsgrouping.iloc[:,1:].astype(str)

#Conditional replacement using mask
mask=df_test<=3.0
mask2=(df_test>=4.0)&(df_test<=6.0)
mask3=(df_test>6.0)
df_test=df_test.where(~mask,'low')
df_test=df_test.where(~mask2,'mid')
df_test=df_test.where(~mask3,'high')


#remove the nan in the list
sectorlist=[x for x in sector if isinstance(x,str)]

#trim the string
myString.strip()

#Get the location of max number of each line of a dataframe
dfrm=dfrm.values                                              #convert to Array
loc=[dfrm.max(axis=1,keepdims=1) == dfrm]     

#Merge 
ER=ER.iloc[ER['date'].isin(dfrank_month['date']),:]   

#Pick column names on conditions:
colnames=df2.columns[(df2.iloc[i,:]<=5)]

#Return Rank horizontally
df2=df2.rank(axis=1,ascending=False,method='min')

#remove from list
a=[x for x in a if x!=20]
[x for x in a if x not in [2, 3, 7]]

#Select row if a columns value is in a list
Ideas=Ideas[Ideas['industry'].isin(Industry)]

#Mask on DataFrame
mask=pd.isnull(CSI300)

#apply function on each row in the column
df['ticker']=df['ticker'].apply(lambda x: x+'.SH' if x[0]=='6' else x+'.SZ')

#Count the occurence of items in a dataframe column:
stats=df['ticker'].value_counts()

#select row, with conditions applied
df=df[df['ticker'].apply(lambda x: len(x)==6)]

#drop duplicates conditional on value in one column
dfcanafterdate=dfcanafterdate.drop_duplicates(['ticker'],keep="first")

#convert a column of int into string
RollingMcap['ticker']=RollingMcap['ticker'].apply(str)

#return the location of a specific item in the column
CSI300.loc[CSI300['Date']==date,:].index[0]

#if else in list comprehension
[x+'.SH' if x.startswith('6') else x+'.SZ' for x in tickerlist]

#if else in apply to dataframe column
pos['ticker']=pos['ticker'].apply(lambda x:x+'.SH' if x.startswith('6') else x+'.SZ')

#Column round to 3 decimals
SPNLM['monthAlpha']=SPNLM['monthAlpha'].round(3)

#Apply functions to two columns
SPNLM[['StratCml','CSI300Cml']]=SPNLM[['StratCml','CSI300Cml']].apply(lambda x: x/x.shift(1)-1)

#convert index of a dataframe into the first column
tradetab.reset_index(level=0,inplace=True)

#remove nan row in a column
newtradetab=newtradetab.loc[newtradetab['date'].isnull()==False,:]

#Return the column name of the minimun column
df.idxmin(axis=1)

#choose elements in list A but not in B
growthsiglist=[x for x in siglist if x not in allsiglist]

#Get year of datetime column
df['year']=df['enddate'].dt.year

#Remove rows that contain a specific value
df[~df['ticker'].str.contains(".HK")==True]

#MID in excel to the whole column
df['ticker']=df['ticker'].str[:6]

#Apply function to columns (axis=1)
sector_analyst_top=sector_analyst_rank.apply(lambda s: s.nlargest(5).index,axis=1)

#Return the index of the first occurence 
 rechist.loc[rechist['rebalday']==rebalday].iloc[[0,]].index[0]
 
 #A yearago of a date, convert it to datetimestamp and convert it back to string
 from dateutil.relativedelta import relativedelta
 membstartdate=str(pd.to_datetime(startdate)-relativedelta(years=1))[0:10]
 
 #use one line loup for dictionary
 tempdict={rebalday:tickerlist for rebalday in rebaldaylist}
 
 equivalent to 
 for rebalday in rebaldaylist:
        tempdict[rebalday]=tickerlist
        
#difference of two lists
   list(set(temp1) - set(temp2))      
   
#Apply functions to dictionaries k is lambda v is the proxy 
   newd={k:pd.DataFrame(v,columns=['ticker','mcap','Q','date'])for k,v in portdict.items()}
   newD = {k:round(v) for k, v in d.items()}
   
#Drop the rows where all elements are missing.
df.dropna(how='all')

#Keep only the rows with at least 2 non-NA values.
df.dropna(thresh=2)

#select rows starting with a chractre
df=df.loc[df['ticker'].str[0].isin(['6','0'])].copy()

#Get the year of date in strings
signaltab['oneyearago']=(signaltab['enddate'].str[0:4].astype(int)-1)

#Return the nth occurence of a value in column:
sighist['nthoccur']=sighist.groupby('index').cumcount()+1

#Map function to two dataframes:(DS.Addindex is a function that takes df1 as input)
df1,df2=map(DS.Addindex,(df1,df2))

#Lambda to multiple dataframe
N2,Sec,N2,HS=map(lambda df: df[['date','ticker','index']],[N2,Sec,NS,HS])

#Append multiple dataframe
frames = [df1, df2, df3]
result = pd.concat(frames)

#Remove a list from another list
CSIcol=[x for x in CSI.columns if x not in['date','ticker','weight','mcap']]

#flatten a list
from itertools import chain
tickerlist=list(set(list(chain(*tickers))))

#remove a list from another list
 Xset=list(set(Xset).difference(['date','ticker']))