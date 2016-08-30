import time
from datetime import date
from datetime import datetime, timedelta

per = 0.1
# 过滤ST及其他具有退市标签的股票
def filter_st_stock(initial_stocks):
    
    df_st_info = get_extras('is_st',initial_stocks,start_date=datetime.now(),end_date=datetime.now())
    df_st_info = df_st_info.T
    df_st_info.rename(columns={df_st_info.columns[0]:'is_st'}, inplace=True)
    unsuspened_stocks = list(df_st_info.index[df_st_info.is_st == False])
    return unsuspened_stocks
    
        
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
    yearL = [year-i for i in range(5)]
    q_PE_G2 = query(valuation.code, valuation.capitalization, cash_flow.pubDate,indicator.eps,\
	            cash_flow.subtotal_operate_cash_inflow, cash_flow.subtotal_operate_cash_outflow,\
	            indicator.inc_operation_profit_year_on_year
                 ).filter(valuation.code.in_(list_stock))
    yearP = [get_fundamentals(q_PE_G2, statDate=str(yearL[i])) for i in range(5)]
    
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
    #columns=[u'名称']+['%d EPS'% (yearL[3-i]) for i in range(4)] 2013 2014 2015
    # yearP0 = get_fundamentals(q_PE_G2, statDate=str(yearL[0]))
    #print yearP[0][(yearP[0].code=='000001.XSHE')]
    results = []
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
            eps[j] = yearP[j][yearP[j].code==i]['eps'].values
        # eps = [ yearP[j][ (yearP[j].code==i).loc[0, 'eps'] ] for j in range(4) ]
        # print eps
        cap = [1]*4
        for j in range(4):
            cap[j] = yearP[j][yearP[j].code==i]['capitalization'].values
            
        flag = False        
        for j in range(3):
            if eps[j]*cap[j] < (1+per)*eps[j+1]*cap[j+1]:
                break
            flag = True    
        if flag:
            #results.append([['%.2f'%eps[3-j]*cap[3-j]/cap[0]  for j in range(3)]])
            gg_price = get_price(i, start_date=last_year, end_date=datetime.now(), frequency='daily', fields='close')
            dpqd1 = (dp_price['close'][last_month]-dp_price['close'][last_2month])/dp_price['close'][last_2month]
            ggqd1 = (gg_price['close'][last_month]-gg_price['close'][last_2month])/gg_price['close'][last_2month]
            dpqd12 = (dp_price['close'][last_month]-dp_price['close'][last_year])/dp_price['close'][last_year]
            ggqd12 = (gg_price['close'][last_month]-gg_price['close'][last_year])/gg_price['close'][last_year]
            xdqd12 = (ggqd12 - dpqd12)/ abs(dpqd12)
            xdqd1 = (ggqd1 - dpqd1)/ abs(dpqd1)
            #+ datetime.timedelta(days = -1)
            results.append([i] + [xdqd1, xdqd12] + ['%.2f'%(eps[3-j]*cap[3-j]/cap[0])  for j in range(4)])
            # print last_year
    print results
# 获得买入信号
# 输出：list_to_buy为list类型,表示待买入的g.num_stocks支股票
def stocks_can_buy():
    list_to_buy = []
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    df_PEG = get_PEG( get_growth_stock( g.feasible_stocks))
    # 将股票按PEG升序排列，返回daraframe类型
    df_sort_PEG = df_PEG.sort(columns=[0], ascending=[1])
    # 将存储有序股票代码index转换成list并取前g.num_stocks个为待买入的股票，返回list
        
    for i in range(len(df_PEG.index)):
        if df_sort_PEG.ix[i,0] < 0.75:
            list_to_buy.append(df_sort_PEG.index[i])
    return list_to_buy

feasible_stocks = filter_st_stock(get_index_stocks('000300.XSHG')) 
feasible_stocks = get_growth_stock(feasible_stocks)