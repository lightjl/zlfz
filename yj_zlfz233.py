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
def set_feasible_stocks(initial_stocks,context):
    # 判断初始股票池的股票是否停牌，返回list
    unsuspened_stocks =filter_paused_stock(initial_stocks)    
    unsuspened_stocks = filter_st_stock(unsuspened_stocks)
    return unsuspened_stocks

# 过滤停牌股票
def filter_paused_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list if not current_data[stock].paused]
    
# 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list 
        if not current_data[stock].is_st 
        and 'ST' not in current_data[stock].name 
        and '*' not in current_data[stock].name 
        and '退' not in current_data[stock].name]
    
       
czg=pd.DataFrame() 

#6
# 计算股票的PEG值
# 输入：context(见API)；stock_list为list类型，表示股票池
# 输出：df_PEG为dataframe: index为股票代码，data为相应的PEG值
def get_PEG(context, stock_list): 
    # 查询股票池里股票的市盈率，收益增长率
    q_PE_G = query(valuation.code, valuation.pe_ratio, indicator.inc_operation_profit_year_on_year
                 ).filter(valuation.code.in_(stock_list)) 
    # 得到一个dataframe：包含股票代码、市盈率PE、收益增长率G
    # 默认date = context.current_dt的前一天,使用默认值，避免未来函数，不建议修改
    df_PE_G = get_fundamentals(q_PE_G)
    # 筛选出成长股：删除市盈率或收益增长率为负值的股票
    df_Growth_PE_G = df_PE_G[(df_PE_G.pe_ratio >0)&(df_PE_G.inc_operation_profit_year_on_year >0)]
    # 去除PE或G值为非数字的股票所在行
    df_Growth_PE_G.dropna()
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
    
def get_growth_stock( stock_list): 
    # 查询股票池里股票的市盈率，收益增长率 indicator.inc_operation_profit_year_on_year
    q_PE_G = query(valuation.code, valuation.pe_ratio, indicator.inc_operation_profit_year_on_year
                 ).filter(valuation.code.in_(stock_list)) 
    # 得到一个dataframe：包含股票代码、市盈率PE、收益增长率G
    # 默认date = context.current_dt的前一天,使用默认值，避免未来函数，不建议修改
    df_PE_G = get_fundamentals(q_PE_G)
    # 筛选出成长股：删除市盈率或收益增长率为负值的股票
    df_Growth_PE_G = df_PE_G[(df_PE_G.pe_ratio >0)&(df_PE_G.inc_operation_profit_year_on_year >0)]
    # 去除PE或G值为非数字的股票所在行
    df_Growth_PE_G.dropna()


    now = datetime.now()  
    year = now.year
    month = now.month
    list_stock = list(df_Growth_PE_G.loc[:,'code'])
    
    #K70 	房地产业，去掉
    list_fdc = get_industry_stocks('K70')
    for i in list_fdc:
        if i in list_stock:
            list_stock.remove(i)
    
    now = datetime.now()  
    year = now.year-1                   #last year
    yearL = [year-i for i in range(5)]  # 2015 2014 2013
    
    q_PE_G2 = query(valuation.code, valuation.capitalization, cash_flow.pubDate,income.basic_eps,\
	            cash_flow.subtotal_operate_cash_inflow, cash_flow.subtotal_operate_cash_outflow,\
	            indicator.inc_operation_profit_year_on_year,\
                balance.total_liability, balance.total_sheet_owner_equities
                 ).filter(valuation.code.in_(list_stock))
    '''    
    q_PE_G2 = query(valuation.code, valuation.capitalization, cash_flow.pubDate,indicator.eps,\
	            cash_flow.subtotal_operate_cash_inflow, cash_flow.subtotal_operate_cash_outflow,\
	            indicator.inc_operation_profit_year_on_year
                 ).filter(valuation.code.in_(list_stock))
    '''             
    yearP = [get_fundamentals(q_PE_G2, statDate=str(yearL[i])) for i in range(5)]
    q_now = query(valuation.code, valuation.capitalization, valuation.pe_ratio)
    df_now = get_fundamentals(q_now)
    last_month = date(datetime.now().year,datetime.now().month,1)-timedelta(1) 
    last_2month = date(datetime.now().year,datetime.now().month-1,1)-timedelta(1)
    last_year = date(datetime.now().year-1,datetime.now().month,1)-timedelta(1) 
    dp_price = get_price('000001.XSHG', start_date=last_year, end_date=last_month+timedelta(1) , frequency='daily', fields='close')
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
        
        # print yearP[0][(yearP[0].code==i)]
        # print i
        flag_empty = False
        for j in range(4):
            if yearP[j][yearP[j].code==i].empty:
                flag_empty = True
        if flag_empty:
            continue
        
        eps = [1]*4
        for j in range(4):
            eps[j] = yearP[j][yearP[j].code==i]['basic_eps'].values[0]
        if eps[3]<0:
            continue
        # eps = [ yearP[j][ (yearP[j].code==i).loc[0, 'basic_eps'] ] for j in range(4) ]
        # print eps
        cap = [1]*4
        for j in range(4):
            cap[j] = yearP[j][yearP[j].code==i]['capitalization'].values[0]
        cap_now = df_now[df_now.code==i]['capitalization'].values[0]
        flag = True        
        for j in range(3):      # 0:now 1:last
            if eps[j]*cap[j] < (1+per)*eps[j+1]*cap[j+1]:
                flag = False
                break
        xjl =  yearP[0][yearP[0].code==i]['subtotal_operate_cash_inflow'].values - \
            yearP[0][yearP[0].code==i]['subtotal_operate_cash_outflow'].values
        xjlb = round(xjl/cap[0]/10000/eps[0],2)
        if flag:
            #results.append([['%.2f'%eps[3-j]*cap[3-j]/cap[0]  for j in range(3)]])
            gg_price = get_price(i, start_date=last_year, end_date=datetime.now(), frequency='daily', fields='close')
            # print gg_price['close'].describe()['max']
            high_price = gg_price['close'].describe()['max']
            low_price = gg_price['close'].describe()['min']
            dpqd1 = (dp_price['close'][last_month]-dp_price['close'][last_2month])/dp_price['close'][last_2month]
            ggqd1 = (gg_price['close'][last_month]-gg_price['close'][last_2month])/gg_price['close'][last_2month]
            dpqd12 = (dp_price['close'][last_month]-dp_price['close'][last_year])/dp_price['close'][last_year]
            ggqd12 = (gg_price['close'][last_month]-gg_price['close'][last_year])/gg_price['close'][last_year]
            xdqd12 = round((ggqd12 - dpqd12)/ abs(dpqd12),2)
            xdqd1 = round((ggqd1 - dpqd1)/ abs(dpqd1),2)
            zcfzl = round(100*yearP[0][yearP[0].code==i]['total_liability'].values / yearP[0][yearP[0].code==i]['total_sheet_owner_equities'].values,2)
            #+ datetime.timedelta(days = -1)
            #results.append([i, all_stcok.ix[i].display_name.replace(' ', '')] + [xdqd1, xdqd12] + ['%.2f'%(eps[3-j]*cap[3-j]/cap[0])  for j in range(4)] + [xjl] )
            '''
            pe_ratio = df_now[df_now.code==i]['pe_ratio'].values[0]
            eps_next = round(gg_price['close'][-2]/pe_ratio,2)
            PEG = pe_ratio/((eps_next-eps[0])/eps[0]*100)
            '''
            num_score = 0
            if xdqd12 > xdqd1 and xdqd1 > 0:
                num_score += 1
            if xjlb > 1:
                num_score += 1
            if zcfzl < 50:
                num_score += 1
            if num_score >= 2:
                results.append([i, all_stcok.ix[i].display_name.replace(' ', '')] + [xdqd1, xdqd12, gg_price['close'][-2]] + ['%.2f'%(eps[3-j]*cap[3-j]/cap_now)  for j in range(1,4)] + [xjlb, low_price, high_price, zcfzl, cap[0], cap_now, num_score] )
                list_pick.append(i)
            # print results
    columns=[u'code', u'名称', u'1月强度', u'1年强度']+[(datetime.now()-timedelta(2)).strftime("%m-%d") ] + ['%dEPS'% (yearL[3-i]) for i in range(1,4)] + [ u'现金比', u'12L', u'12H', u'负债率', u'上年股本', u'现股本', u'分数']
    # 
    czg = pd.DataFrame(data=results, columns=columns)
    czg.sort(columns=u'分数', ascending = False, inplace=True)
    
    df_PEG = get_PEG(list_pick)
    
    df_sort_PEG = df_PEG.sort(columns=[0], ascending=[1])
    # 将存储有序股票代码index转换成list并取前g.num_stocks个为待买入的股票，返回list

    #nummax = min(len(df_PEG.index), g.num_stocks-len(.portfolio.positions.keys()))
    
    list_can_buy = []    
    for i in range(len(df_sort_PEG.index)):
        if df_sort_PEG.ix[i,0] < 0.6:
            list_can_buy.append(df_sort_PEG.index[i])
        else:
            break
    if len(list_can_buy) < 3:
        for i in range(len(df_sort_PEG.index)):
            if df_sort_PEG.ix[i,0] >= 0.6 and df_sort_PEG.ix[i,0] < 0.75:
                list_can_buy.append(df_sort_PEG.index[i])
    czg_buy = pd.DataFrame() 
    buy_list = []
    for i in list_can_buy:
        df_buy = czg[czg.code==i]
        buy_list.append([i, df_buy['名称'].values[0], df_sort_PEG.ix[i,0], df_buy['1月强度'].values[0], df_buy['1年强度'].values[0], df_buy[(datetime.now()-timedelta(2)).strftime("%m-%d")].values[0], df_buy['现金比'].values[0], df_buy['12L'].values[0]], df_buy['12H'].values[0]], df_buy['负债率'].values[0], df_buy['上年股本'].values[0], df_buy['现股本'].values[0], df_buy['分数'].values[0] )
    return czg
    #print results
    
#7
# 获得买入信号
# 输入：context(见API)
# 输出：list_to_buy为list类型,表示待买入的g.num_stocks支股票
def stocks_can_buy():
    list_can_buy = []
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    df_PEG = get_PEG( get_growth_stock(g.feasible_stocks))
    # 将股票按PEG升序排列，返回daraframe类型
    df_sort_PEG = df_PEG.sort(columns=[0], ascending=[1])
    # 将存储有序股票代码index转换成list并取前g.num_stocks个为待买入的股票，返回list

    #nummax = min(len(df_PEG.index), g.num_stocks-len(.portfolio.positions.keys()))
        
    for i in range(len(df_sort_PEG.index)):
        if df_sort_PEG.ix[i,0] < 0.6:
            list_can_buy.append(df_sort_PEG.index[i])
        else:
            break
    if len(list_can_buy) < 3:
        for i in range(len(df_sort_PEG.index)):
            if df_sort_PEG.ix[i,0] >= 0.6 and df_sort_PEG.ix[i,0] < 0.75:
                list_can_buy.append(df_sort_PEG.index[i])

    return list_can_buy

feasible_stocks = set_feasible_stocks(get_all_securities(['stock']).index) 
czg = get_growth_stock(feasible_stocks)
czg