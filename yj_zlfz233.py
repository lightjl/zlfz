'''
现金比,负债率 上年
'''
import time
from datetime import date
from datetime import datetime, timedelta
import pandas as pd
all_stcok = get_all_securities(['stock'])
per = 0.1

#4
# 设置可行股票池：过滤掉当日停牌的股票
# 输入：initial_stocks为list类型,表示初始股票池； context（见API）
# 输出：unsuspened_stocks为list类型，表示当日未停牌的股票池，即：可行股票池
def set_feasible_stocks(initial_stocks):
    # 判断初始股票池的股票是否停牌，返回list  
    unsuspened_stocks = filter_st_stock(initial_stocks)
    return unsuspened_stocks

# 过滤ST及其他具有退市标签的股票
def filter_st_stock(initial_stocks):
    
    df_st_info = get_extras('is_st',initial_stocks,start_date=datetime.now(),end_date=datetime.now())
    df_st_info = df_st_info.T
    df_st_info.rename(columns={df_st_info.columns[0]:'is_st'}, inplace=True)
    unsuspened_stocks = list(df_st_info.index[df_st_info.is_st == False])
    return unsuspened_stocks

#6
# 计算股票的PEG值
# 输入：context(见API)；stock_list为list类型，表示股票池
# 输出：df_PEG为dataframe: index为股票代码，data为相应的PEG值
# flag_pick 是否挑选，是挑选
def get_PEG(stock_list, flag_pick, year, month): 
    # 查询股票池里股票的市盈率，营业利润同比增长率
    q_PE_G = query(valuation.code, valuation.pe_ratio, indicator.inc_operation_profit_year_on_year
                 ).filter(valuation.code.in_(stock_list)) 
    # 得到一个dataframe：包含股票代码、市盈率PE、收益增长率G
    # 默认date = context.current_dt的前一天,使用默认值，避免未来函数，不建议修改
    df_PE_G = get_fundamentals(q_PE_G, date=str(year)+'-'+str(month)+'-'+str(1))
    # 筛选出成长股：删除市盈率或收益增长率为负值的股票
    if flag_pick:
        df_Growth_PE_G = df_PE_G
    else:
        df_Growth_PE_G = df_PE_G[(df_PE_G.pe_ratio >0)&(df_PE_G.inc_operation_profit_year_on_year >0)]
    # 去除PE或G值为非数字的股票所在行
    df_Growth_PE_G.dropna()
    
    # print df_Growth_PE_G
    
    # 得到一个Series：存放股票的市盈率TTM，即PE值
    Series_PE = df_Growth_PE_G.ix[:,'pe_ratio']
    # 得到一个Series：存放股票的收益增长率，即G值
    Series_G = df_Growth_PE_G.ix[:,'inc_operation_profit_year_on_year']
    # 得到一个Series：存放股票的PEG值
    Series_PEG = Series_PE/Series_G
    # 将股票与其PEG值对应
    Series_PEG.index = df_Growth_PE_G.ix[:,0]
    # 将Series类型转换成dataframe类型
    df_PEG = pd.DataFrame(Series_PEG)
    return df_PEG
# flag_pick 是否跳过挑选，是不挑选
def get_growth_stock(stock_list, flag_result, flag_pick, year, month): 
    print 'start'
    pe_ration_max = 40
    # 查询股票池里股票的市盈率，收益增长率 indicator.inc_operation_profit_year_on_year
    q_PE_G = query(valuation.code, valuation.pe_ratio, indicator.inc_operation_profit_year_on_year
                 ).filter(valuation.code.in_(stock_list)) 
    # 得到一个dataframe：包含股票代码、市盈率PE、收益增长率G
    # 默认date = context.current_dt的前一天,使用默认值，避免未来函数，不建议修改
    df_PE_G = get_fundamentals(q_PE_G, date=str(year)+'-'+str(month)+'-'+str(1))
    # 筛选出成长股：删除市盈率或收益增长率为负值的股票


    if not flag_pick:
        df_Growth_PE_G = df_PE_G[(df_PE_G.pe_ratio>0)&(df_PE_G.pe_ratio<pe_ration_max)&(df_PE_G.inc_operation_profit_year_on_year >20)]
    else:
        df_Growth_PE_G = df_PE_G
    # 去除PE或G值为非数字的股票所在行
    df_Growth_PE_G.dropna()
    
    list_stock = list(df_Growth_PE_G.loc[:,'code'])

    #K70 	房地产业，去掉
    list_fdc = get_industry_stocks('K70')
    for i in list_fdc:
        if i in list_stock:
            list_stock.remove(i)
            
    
    # 去掉超大盘 000043.XSHG 399980.XSHE
    cdp = get_index_stocks('000043.XSHG')
    for i in cdp:
        if i in list_stock:
            list_stock.remove(i)
    cdp = get_index_stocks('399980.XSHE')
    for i in cdp:
        if i in list_stock:
            list_stock.remove(i)
            
    
    q_PE_G2 = query(valuation.code, valuation.capitalization, \
                cash_flow.pubDate,income.basic_eps, valuation.pe_ratio,\
	            cash_flow.subtotal_operate_cash_inflow, \
                cash_flow.subtotal_operate_cash_outflow,\
	            indicator.inc_operation_profit_year_on_year,\
                balance.total_liability, balance.total_sheet_owner_equities
                 ).filter(valuation.code.in_(list_stock))

    yearP1 = get_fundamentals(q_PE_G2, statDate=str(year-1))
    yearL = [year-5+i for i in range(5)]  # 2011 2012 2013 2014 2015 今年2016
    
    '''    
    q_PE_G2 = query(valuation.code, valuation.capitalization, cash_flow.pubDate,indicator.eps,\
	            cash_flow.subtotal_operate_cash_inflow, cash_flow.subtotal_operate_cash_outflow,\
	            indicator.inc_operation_profit_year_on_year
                 ).filter(valuation.code.in_(list_stock))
    '''             
    yearP = [get_fundamentals(q_PE_G2, statDate=str(yearL[i])) for i in range(5)]
    
    q_now = query(valuation.code, valuation.capitalization, valuation.pe_ratio)
    df_now = get_fundamentals(q_now, date=str(year)+'-'+str(month)+'-'+str(1))
    last_month = date(year,month,1)-timedelta(1) 
    if month > 1:
         last_2month = date(year,month-1,1)-timedelta(1)
    else:
         last_2month = date(year-1,12,1)-timedelta(1)
    last_year = date(year-1,month,1)-timedelta(1) 

    dp_price = get_price('000001.XSHG', start_date=last_year-timedelta(20), \
        end_date=last_month+timedelta(1) , frequency='daily', fields='close')
    while last_month not in dp_price.index:
        last_month = last_month-timedelta(1) 
    while last_2month not in dp_price.index:
        last_2month = last_2month-timedelta(1) 
    while last_year not in dp_price.index:
        last_year = last_year-timedelta(1)
    # print dp_price
    # print yearP[1]
    #todo： 去年全部 年报已公开，明年昨日再改
    # 2013 2014 2015
    # print columns
    # yearP0 = get_fundamentals(q_PE_G2, statDate=str(yearL[0]))
    #print yearP[0][(yearP[0].code=='000001.XSHE')]
    results = []
    list_pick = []
    
    for i in list_stock:
            
        start_yearth = 1
        if yearP1[yearP1.code==i].empty:
            start_yearth = 0
        elif now.strftime("%Y-%m-%d") < yearP1[yearP1.code==i]['pubDate'].values[0]:
            start_yearth = 0
            # 未公布
        # print yearP[0][(yearP[0].code==i)]
        # print i
        flag_empty = False
        for j in range(start_yearth, 4+start_yearth):
            if yearP[j][yearP[j].code==i].empty:
                flag_empty = True
                break
        if flag_empty:
            continue
        
        
        eps = [1]*4
        for j in range(4):
            eps[j] = yearP[j+start_yearth][yearP[j+start_yearth].code==i]['basic_eps'].values[0]
        if eps[0]<0:
            continue
        
            
        # eps = [ yearP[j][ (yearP[j].code==i).loc[0, 'basic_eps'] ] for j in range(4) ]
        # print eps
        cap = [1]*4
        for j in range(4):
            cap[j] = yearP[j+start_yearth][yearP[j+start_yearth].code==i]['capitalization'].values[0]
        cap_now = df_now[df_now.code==i]['capitalization'].values[0]
        pe_now = df_now[df_now.code==i]['pe_ratio'].values[0]
        flag_cz = True         
        for j in range(3):
            if (1+per)*eps[j]*cap[j] > eps[j+1]*cap[j+1]:
                flag_cz = False
                break
        df_lastyear = yearP[3+start_yearth][yearP[3+start_yearth].code==i]
        if flag_cz or flag_pick:
            #results.append([['%.2f'%eps[3-j]*cap[3-j]/cap[0]  for j in range(3)]])
            gg_price = get_price(i, start_date=last_year, end_date=now, frequency='daily', fields='close')
            scoreOfStock = 0
            zcfzl = round(df_lastyear['total_liability'].values[0]/df_lastyear['total_sheet_owner_equities'].values[0],2)
            if df_lastyear['pe_ratio'].values[0] <= pe_ration_max or flag_pick:
                if zcfzl<0.5:
                    scoreOfStock += 1
                xjl = df_lastyear['subtotal_operate_cash_inflow'].values[0] - \
                    df_lastyear['subtotal_operate_cash_outflow'].values[0]
                xjlb = round(xjl/cap[3]/10000/eps[3],2)
                
                if df_lastyear['subtotal_operate_cash_inflow'].values[0] - df_lastyear['subtotal_operate_cash_outflow'].values[0] > df_lastyear['basic_eps'].values[0]*df_lastyear['capitalization'].values[0]*10000:
                    scoreOfStock += 1
            # print gg_price['close'].describe()['max']
                high_price = gg_price['close'].describe()['max']
                low_price = gg_price['close'].describe()['min']
                dpqd1 = (dp_price['close'][last_month]-dp_price['close'][last_2month])/dp_price['close'][last_2month]
                ggqd1 = (gg_price['close'][last_month]-gg_price['close'][last_2month])/gg_price['close'][last_2month]
                dpqd12 = (dp_price['close'][last_month]-dp_price['close'][last_year])/dp_price['close'][last_year]
                ggqd12 = (gg_price['close'][last_month]-gg_price['close'][last_year])/gg_price['close'][last_year]
                xdqd12 = round((ggqd12 - dpqd12)/ abs(dpqd12),2)
                xdqd1 = round((ggqd1 - dpqd1)/ abs(dpqd1),2)

                #+ datetime.timedelta(days = -1)
                # results.append([i, all_stcok.ix[i].display_name.replace(' ', '')] + [xdqd1, xdqd12] + ['%.2f'%(eps[3-j]*cap[3-j]/cap[0])  for j in range(4)] + [xjl] )

                if xdqd12 > xdqd1 and xdqd1 > 0:
                    scoreOfStock += 1

                if scoreOfStock >= 2 or flag_pick:
                    list_pick.append(i)
                    if flag_result:
                        results.append([i, all_stcok.ix[i].display_name.replace(' ', '')] + [pe_now, xdqd1, xdqd12, gg_price['close'][-2]] + ['%.2f'%(eps[j]*cap[j]/cap_now)  for j in range(4)] + [xjlb, low_price, high_price, zcfzl, cap[3], cap_now, scoreOfStock] )

    if flag_result:
        columns=[u'code', u'名称', u'PE', u'1月强度', u'1年强度']+[(datetime.now()-timedelta(1)).strftime("%m-%d") ] + ['%dEPS'% (-4+j) for j in range(4)] + [ u'现金比', u'12L', u'12H', u'负债率', u'上年股本', u'现股本', u'分数']
    # 
        czg = pd.DataFrame(data=results, columns=columns)
        czg.sort(columns=u'分数', ascending = False, inplace=True)
        #print czg
        
    # flag_pick 是否跳过挑选，是不挑选
    df_PEG = get_PEG(list_pick, flag_pick, year, month)
    
    df_sort_PEG = df_PEG.sort(columns=[0], ascending=[1])
    # 将存储有序股票代码index转换成list并取前g.num_stocks个为待买入的股票，返回list
    #nummax = min(len(df_PEG.index), g.num_stocks-len(.portfolio.positions.keys()))
    
    list_can_buy = []    
    for i in range(len(df_sort_PEG.index)):
        if df_sort_PEG.ix[i,0] < 0.6 or flag_pick:
            list_can_buy.append(df_sort_PEG.index[i])
        else:
            break
    if not flag_pick:
        if len(list_can_buy) < 10:
            for i in range(len(df_sort_PEG.index)):
                if df_sort_PEG.ix[i,0] >= 0.6 and df_sort_PEG.ix[i,0] < 0.75:
                    list_can_buy.append(df_sort_PEG.index[i])

    
    if flag_result:
        czg_buy = pd.DataFrame() 
    
        buy_list = []
        for i in list_can_buy:
            df_buy = czg[czg.code==i]
            
            buy_list.append([ i, df_buy[u'名称'].values[0], df_buy[u'PE'].values[0], \
                round(df_sort_PEG.ix[i,0],2), \
                df_buy[u'1月强度'].values[0], df_buy[u'1年强度'].values[0] ] 
                + [df_buy[str(-4)+'EPS'].values[0] ] \
                + [df_buy[str(-3)+'EPS'].values[0] ] \
                + [df_buy[str(-2)+'EPS'].values[0] ] \
                + [df_buy[str(-1)+'EPS'].values[0] ] \
                + [df_buy[(datetime.now()-timedelta(1)).strftime("%m-%d")].values[0], \
                df_buy[u'现金比'].values[0], df_buy[u'12L'].values[0], 
                df_buy[u'12H'].values[0], df_buy[u'负债率'].values[0], \
                df_buy[u'上年股本'].values[0], df_buy[u'现股本'].values[0], \
                df_buy[u'分数'].values[0] ] )
        
        columns2=[u'code', u'名称', u'PE', u'PEG', u'1月强度', u'1年强度'] \
            + ['%dEPS'% (-4+j) for j in range(4)] \
            + [(datetime.now()-timedelta(1)).strftime("%m-%d") ] \
            + [ u'现金比', u'12L', u'12H', u'负债率', u'上年股本', u'现股本', u'分数']
        czg_buy = pd.DataFrame(data=buy_list, columns=columns2)
        return czg_buy
    return list_can_buy
    #print results

'''
list1 = get_all_securities(['stock']).index[:1000]
list2 = get_all_securities(['stock']).index[1001:2000]
list3 = get_all_securities(['stock']).index[2001:]
list1 = ['600027.XSHG', '002367.XSHE', '002508.XSHE']
'''


list1 = get_all_securities(['stock']).index[:1000]
list2 = get_all_securities(['stock']).index[1001:2000]
list3 = get_all_securities(['stock']).index[2001:]

now = datetime.now()  
year = now.year
month = now.month
year = 2015
month = 5

flag_st = False
# get_growth_stock(list, result, notpick)
if flag_st:
    list1 = set_feasible_stocks(list1) 
    list2 = set_feasible_stocks(list2) 
    list3 = set_feasible_stocks(list3) 
    
result1 = []
result2 = []
result3 = []
result1 = get_growth_stock(list1, False, False, year, month)
result2 = get_growth_stock(list2, False, False, year, month)
result3 = get_growth_stock(list3, False, False, year, month)
results = []

for i in result1:
    if i not in results:
        results.append(i)
for i in result2:
    if i not in results:
        results.append(i)
for i in result3:
    if i not in results:
        results.append(i)
print results
df_czg = get_growth_stock(results, True, False, year, month)
df_czg


# 不过滤
print '---------------------------观察列表-------------------------'
listbefore = ['002202.XSHE', '002372.XSHE', '600114.XSHG', '000501.XSHE', '600522.XSHG', '601009.XSHG', '601199.XSHG']
#
listbefore = ['600027.XSHG', '002367.XSHE', '002508.XSHE']
df_gc = get_growth_stock(listbefore, True, True, year, month)

