# 《彼得林奇的"成功投资"》
# 回测：2006-1-1到2016-5-31，￥1000000 ，每天

import pandas as pd
import time
from datetime import date
from datetime import timedelta
import tradestat
'''
================================================================================
总体回测前
================================================================================
'''
#总体回测前要做的事情
def initialize(context):
    set_params()                             # 设置策略常量
    set_variables()                          # 设置中间变量
    set_backtest()                           # 设置回测条件
    # 加载统计模块
    if g.flag_stat:
        g.trade_stat = tradestat.trade_stat()

#1 
#设置策略参数
def set_params():
    g.num_stocks = 5                             # 每次调仓选取的最大股票数量
    g.stocks=get_all_securities(['stock']).index # 设置上市A股为初始股票池 000002.XSHG
   
    
    g.per = 0.05                                 # EPS增长率不低于0.25
    g.flag_stat = True                           # 默认不开启统计
    g.trade_skill = True                         # 开启交易策略

#2
#设置中间变量
def set_variables():
    return None

#3
#设置回测条件
def set_backtest():
    set_option('use_real_price',True)        # 用真实价格交易
    log.set_level('order','debug')           # 设置报错等级

'''
================================================================================
每天开盘前
================================================================================
'''
#每天开盘前要做的事情
def before_trading_start(context):
    set_slip_fee(context)                 # 设置手续费与手续费
    # 设置可行股票池
    g.feasible_stocks = set_feasible_stocks(g.stocks,context)
    g.feasible_stocks = stocks_can_buy(context)
    log.debug(g.feasible_stocks)
    
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

#5
# 根据不同的时间段设置滑点与手续费
# 输入：context（见API）
# 输出：none
def set_slip_fee(context):
    # 将滑点设置为0
    set_slippage(FixedSlippage(0)) 
    # 根据不同的时间段设置手续费
    dt=context.current_dt
    if dt>datetime.datetime(2013,1, 1):
        set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5)) 
        
    elif dt>datetime.datetime(2011,1, 1):
        set_commission(PerTrade(buy_cost=0.001, sell_cost=0.002, min_cost=5))
            
    elif dt>datetime.datetime(2009,1, 1):
        set_commission(PerTrade(buy_cost=0.002, sell_cost=0.003, min_cost=5))
    else:
        set_commission(PerTrade(buy_cost=0.003, sell_cost=0.004, min_cost=5))


'''
================================================================================
每天交易时
================================================================================
'''
# 每天回测时做的事情
def handle_data(context,data):
    # 待卖出的股票，list类型
    list_to_sell = stocks_to_sell(context, data, g.feasible_stocks)
    # 需买入的股票
    list_to_buy = pick_buy_list(context, data, g.feasible_stocks)
    # 卖出操作
    sell_operation(context,list_to_sell)
    # 买入操作
    buy_operation(context, list_to_buy)
    
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
    
# 挑选净利润同比增长率连续3年大于0.25的
# 输入：context(见API)；stock_list为list类型，表示初始股票池
# 输出：buy_list_stocks为list: 为股票代码
def get_growth_stock(context, stock_list): 
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

    list_stock = list(df_Growth_PE_G.loc[:,'code'])
    #K70 	房地产业    J66 货币金融服务
    list_fdc = get_industry_stocks('K70')
    list_hbjr = get_industry_stocks('J66')
    
    # 去掉房地产
    for i in list_fdc:
        if i in list_stock:
            list_stock.remove(i)
    
    q_PE_G2 = query(valuation.code, valuation.capitalization, cash_flow.pubDate,\
                indicator.eps, cash_flow.subtotal_operate_cash_inflow,\
                cash_flow.subtotal_operate_cash_outflow,\
	            indicator.inc_operation_profit_year_on_year, \
                valuation.pe_ratio, balance.total_liability, \
                balance.total_sheet_owner_equities, income.basic_eps
                 ).filter(valuation.code.in_(list_stock))

    buy_list_stocks = []
    # 查询股票池里股票的EPS,股本(万股),发布财报日期,eps,
    # 经营活动现金流入(元),经营活动现金流出(元)
    # 净利润同比增长率 ==> 每股现金流
    # 处理过去3年的数据
    # 净利润同比增长率 inc_operation_profit_year_on_year 连续3年>= 10
    # 动态市盈率 <= 50
    # 资产负债率 < 0.5
    # 每股现金流 > EPS(去年)
    # 相对强度 xdqd12 >= xdqd1 >=0	
    pe_ration_max = 50
    
    year = context.current_dt.year
    month = context.current_dt.month
    yearP1 = get_fundamentals(q_PE_G2, statDate=str(year-1))
    yearL = [year-5+i for i in range(5)]  # 2011 2012 2013 2014 2015 今年2016
    yearP = [get_fundamentals(q_PE_G2, statDate=str(yearL[i])) for i in range(5)]
    q_now = query(valuation.code, valuation.capitalization, valuation.pe_ratio)
    df_now = get_fundamentals(q_now)
    last_month = date(year,month,1)-timedelta(1) 
    if month > 1:
        last_2month = date(year,month-1,1)-timedelta(1)
    else:
        last_2month = date(year-1,12,1)-timedelta(1)
    last_year = date(year-1,month,1)-timedelta(1) 
    
    dp_price = get_price('000001.XSHG', start_date=last_year-timedelta(20), end_date=last_month+timedelta(1) , frequency='daily', fields='close')
    
    while last_month not in dp_price.index:
        last_month = last_month-timedelta(1) 
    while last_2month not in dp_price.index:
        last_2month = last_2month-timedelta(1) 
    while last_year not in dp_price.index:
        last_year = last_year-timedelta(1)
    
    
    for i in list_stock:
        # 流通市值>500跳过
        #if yearP1.loc[0,'circulating_market_cap'] > 500:
        #    continue
        #log.info(yearP1)
        startth = 0
        if context.current_dt.strftime("%Y-%m-%d") < yearP1.loc[0,'pubDate']:
            start_yearth = 1
        # 回测当天，前4年的已出的年报有无空的    
        flag_empty = False
        for j in range(startth, 4+startth):
            if yearP[j][yearP[j].code==i].empty:
                flag_empty = True
                break
        if flag_empty:
            continue
        eps = [1]*4     # 2011 2012 2013 2014 or 2012 ... 今年2016
        for j in range(4):
            eps[j] = yearP[j+startth][yearP[j+startth].code==i]['basic_eps'].values[0]
        if eps[0]<0:
            continue
        cap = [1]*4     # 2011 2012 2013 2014 or 2012 ... 今年2016
        for j in range(4):
            cap[j] = yearP[j+startth][yearP[j+startth].code==i]['capitalization'].values[0]
        cap_now = df_now[df_now.code==i]['capitalization'].values[0]
        
        flag_cz = True        
        for j in range(3):      # 2011 2012 2013 2014 or 2012 ... 今年2016
            if (1+g.per)*eps[j]*cap[j] > eps[j+1]*cap[j+1]:
                # 增长不达
                flag_cz = False
                break
        # todo
        if flag_cz:
            #log.info("code=%s, market_cap=%d", i, yearP1.loc[0, 'circulating_market_cap'])
            #log.debug(yearP1)
            #log.debug(yearP2) 
            #log.debug(yearP3)
            #log.debug(yearP4)
            # 动态市盈率 负债合计(元)/负债和股东权益合计
            
            #满分 1+1+1
            scoreOfStock = 0
            # pe_ratio 动态市盈率  负债合计(元)/负债和股东权益合计= 资产负债率 < 0.5
            zcfzl = yearP1.loc[0, 'total_liability']/yearP1.loc[0, 'total_sheet_owner_equities']
            if yearP1.loc[0, 'pe_ratio'] <= pe_ration_max:
                if zcfzl<0.5:
                    scoreOfStock = scoreOfStock + 1
                #每股现金流 > EPS(去年)
                if yearP1.loc[0, 'subtotal_operate_cash_inflow'] - yearP1.loc[0, 'subtotal_operate_cash_outflow'] > yearP1.loc[0, 'basic_eps']*yearP1.loc[0, 'capitalization']:
                    scoreOfStock = scoreOfStock + 1
                # 相对强度
                h = history(260, security_list=['000001.XSHG', i])
                dpqd1 = (h['000001.XSHG'][-1]-h['000001.XSHG'][-22])/h['000001.XSHG'][-22]
                ggqd1 = (h[i][-1]-h[i][-22])/h[i][-22]
                dpqd12 = (h['000001.XSHG'][-1]-h['000001.XSHG'][-260])/h['000001.XSHG'][-260]
                ggqd12 = (h[i][-1]-h[i][-260])/h[i][-260]
                xdqd12 = (ggqd12 - dpqd12)/ abs(dpqd12)
                xdqd1 = (ggqd1 - dpqd1)/ abs(dpqd1)
                if xdqd12 >= xdqd1 and xdqd1 >= 0:
                    scoreOfStock = scoreOfStock + 1
                if scoreOfStock >= 2:
                    buy_list_stocks.append(i)
            #log.info(yearP2)
            #log.info(yearP3)
            #log.info("code=%s, zcfzl=%.2f, sylyc=%f, yearL1 = %d, P1=%f, P2=%f, P3=%f", i, zcfzl, yearP1.loc[0, 'pe_ratio'], \
            #                yearL1, yearP1.loc[0, 'eps'], yearP2.loc[0, 'eps'], yearP3.loc[0, 'eps'])
    return buy_list_stocks
    
#7
# 获得买入信号
# 输入：context(见API)
# 输出：list_to_buy为list类型,表示待买入的g.num_stocks支股票
def stocks_can_buy(context):
    list_can_buy = []
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    df_PEG = get_PEG(context, get_growth_stock(context, g.feasible_stocks))
    # 将股票按PEG升序排列，返回daraframe类型
    df_sort_PEG = df_PEG.sort(columns=[0], ascending=[1])
    # 将存储有序股票代码index转换成list并取前g.num_stocks个为待买入的股票，返回list
    nummax = min(len(df_PEG.index), g.num_stocks-len(context.portfolio.positions.keys()))
        
    for i in range(nummax):
        if df_sort_PEG.ix[i,0] < 0.75:
            list_can_buy.append(df_sort_PEG.index[i])
    return list_can_buy
    
    
 
'''
================================================================================
多均线策略
================================================================================
'''
# 判断均线纠缠
def is_struggle(mavg1,mavg2,mavg3):
    if abs((mavg1-mavg2)/mavg2)< 0.003\
    or abs((mavg2-mavg3)/mavg3)< 0.002:
        return True
    return False
    
# 计算股票过去n个单位时间（一天）均值
# n 天的MA， day_before天前
def count_ma(stock,n, day_before):
    #log.debug(history(n, '1d', 'close', [stock])[stock])
    #log.debug(history(n, '1d', 'close', [stock],df = False)[stock].mean())
    return history(n+day_before, '1d', 'close', [stock],df = False)[stock][0:n].mean()


# 判断多头排列 5 10 20 30
def is_highest_point(data,stock,n):
    if count_ma(stock,5,n) > count_ma(stock,10,n)\
    and count_ma(stock,10,n) > count_ma(stock,20,n)\
    and count_ma(stock,20,n) > count_ma(stock,30,n):
        #log.debug('%s, ma5=%.2f, ma10=%.2f, ma20=%.2f, ma30=%.2f', stock, count_ma(stock,5,-n)\
        #,count_ma(stock,10,-n),count_ma(stock,20,-n),count_ma(stock,30,-n))
        return True
    return False

# 判断空头排列——空仓 5 10 20
def is_lowest_point(data,stock,n):
    if count_ma(stock,5,n) < count_ma(stock,10,n)\
    and count_ma(stock,10,n) < count_ma(stock,20,n):
        log.debug('%d, %s, ma5=%.2f, ma10=%.2f, ma20=%.2f',n, stock, count_ma(stock,5,n), count_ma(stock,10,n), count_ma(stock,20,n))
        return True
    return False
    
    
# 判断10日线， 20日线空头排列后的金叉——买入
def is_crossUP(data,stock,short,long):
    if is_lowest_point(data,stock,1) and is_lowest_point(data,stock,2):
        if count_ma(stock, short, 1) < count_ma(stock, long, 1)\
        and count_ma(stock, short, 0) > count_ma(stock, long, 0):
            return True
    return False

# 判断多头排列后的死叉——卖出
def is_crossDOWN(data,stock,short,long):
    if is_highest_point(data,stock,1) and is_highest_point(data,stock,2):
        if count_ma(stock, short, 1) > count_ma(stock, long, 1)\
        and count_ma(stock, short, 0) < count_ma(stock, long, 0):
            log.debug('%s, MAs-2:%.2f, MAl-2:%.2f; MAs-1:%.2f MAl-1:%.2f', stock, count_ma(stock, short, 1), count_ma(stock, long, 1), count_ma(stock, short, 0), count_ma(stock, long, 0))
            return True
    return False
'''
================================================================================
多均线策略
================================================================================
''' 
# 获得买入的list_to_buy
# 输入list_can_buy 为list，可以买的队列
# 输出list_to_buy 为list，买入的队列
def pick_buy_list(context, data, list_can_buy):
    list_to_buy = []
    buy_num = g.num_stocks - len(context.portfolio.positions.keys())
    if buy_num <= 0:
        return list_to_buy
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    ad_num = 0;
    if g.trade_skill:
        for stock in list_can_buy:
            if stock in context.portfolio.positions.keys():
                continue
            ma20 = count_ma(stock, 20, 0)
            close2day = history(2, '1d', 'close', [stock],df = False)[stock]
            if is_struggle(close2day[-2],close2day[-1],ma20):
                continue
            if close2day[-2] < ma20 and close2day[-1] > ma20:
                list_to_buy.append(stock)
                ad_num += 1
                if ad_num >= buy_num:
                    break
                
            '''
            if stock not in context.portfolio.positions.keys():
                list_to_buy.append(stock)
            # MA60上才考虑买
            ma60 = count_ma(stock, 60, 0)
            if close < ma60:
                continue
            '''
            
            '''
            # 多头排列——满仓买入
            if is_highest_point(data,stock,0):
                # 均线纠缠时，不进行买入操作
                if is_struggle(count_ma(stock,10,0),count_ma(stock,20,0),count_ma(stock,30,0)):
                    continue
                else:
                    if close < ma20:
                        list_to_buy.append(stock)
                        ad_num += 1
                        if ad_num >= buy_num:
                            break
                        continue
            # 空头排列后金叉——满仓买入 10, 20 金叉
            if is_crossUP(data,stock,10, 20):
                if is_struggle(count_ma(stock,10,0), \
                    count_ma(stock,20,0),count_ma(stock,30,0)):
                    continue
                list_to_buy.append(stock)
                ad_num += 1
                if ad_num >= buy_num:
                    break
            '''
    else:
        for stock in list_can_buy:
            if stock in context.portfolio.positions.keys():
                continue
            list_to_buy.append(stock)
            ad_num += 1
            if ad_num >= buy_num:
                break
            
    return list_to_buy

# 已不再具有持有优势的股票
# 输入：context(见API)；stock_list_now为list类型，表示初始持有股, stock_list_buy为list类型，表示可以买入的股票
# 输出：应清仓的股票 list
def get_clear_stock(context, stock_list_buy):
    # 

    stock_hold = []
    year = context.current_dt.year
    stock_list_now = context.portfolio.positions.keys()
    for i in stock_list_now:
        q_PE_G = query(valuation.capitalization, income.basic_eps, cash_flow.pubDate
                 ).filter(valuation.code == i)

        jbm = get_fundamentals(q_PE_G, statDate=str(year-1))
        jbmP2 = get_fundamentals(q_PE_G, statDate=str(year-2))
        if context.current_dt.strftime("%Y-%m-%d") <= jbm.loc[0,'pubDate']:
            jbm = get_fundamentals(q_PE_G, statDate=str(year-2))
            jbmP2 = get_fundamentals(q_PE_G, statDate=str(year-3))
        #log.info(jbm)
        #log.info(jbmP2)
        if jbm.loc[0, 'basic_eps']*jbm.loc[0, 'capitalization'] >= (1+g.per)*jbmP2.loc[0, 'basic_eps']*jbmP2.loc[0, 'capitalization']:
            stock_hold.append(i)
        else:
            log.info("code=%s, jbm=%f, jbmP2=%f", i, jbm.loc[0, 'basic_eps']*jbm.loc[0, 'capitalization'], jbmP2.loc[0, 'basic_eps']*jbmP2.loc[0, 'capitalization'])
        #log.info("code=%s, jbm=%f, jbmP2=%f", i, jbm.loc[0, 'basic_eps']*jbm.loc[0, 'capitalization'], (1+g.per)*jbmP2.loc[0, 'basic_eps']*jbmP2.loc[0, 'capitalization'])

    list_to_sell = []
    #log.info(stock_hold)
    stock_can_buy = stock_hold      # +stock_list_buy
    for i in stock_list_buy:
        stock_can_buy.append(i)
    #log.info(stock_can_buy)
    #全都不值的保留
    if stock_can_buy is None:
        return stock_list_now
    news_ids = []
    for id in stock_can_buy:
        if id not in news_ids:
            news_ids.append(id)
    #log.debug(news_ids, "stock_can_buy")
    
    
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    df_PEG = get_PEG(context, news_ids)
    # 将股票按PEG升序排列，返回daraframe类型
    df_sort_PEG = df_PEG.sort(columns=[0], ascending=[1])
    # 将存储有序股票代码index转换成list并取前g.num_stocks个为待买入的股票，返回list
    nummax = min(len(df_PEG.index), g.num_stocks)
    stock_hold = list(df_sort_PEG[0:g.num_stocks].index)
    # 如果持有不应持有的股票卖出
    for i in stock_list_now:
        if i not in stock_hold:
            list_to_sell.append(i)
    
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    df_PEG = get_PEG(context, stock_list_now)
    # 将股票按PEG升序排列，返回daraframe类型
    df_sort_PEG = df_PEG.sort(columns=[0], ascending=[1])

    len_df = len(df_PEG.index)
    for i in range(len_df):
        if df_sort_PEG.ix[i,0] > 1.2:
            list_to_sell.append(df_sort_PEG.index[i])
            #log.info("code=%s PEG>1.2", i)
            
    return list_to_sell
# 获得均线卖出信号
# 输入：context（见API文档）
# 输出：list_to_sell为list类型，表示待卖出的股票
def stocks_djx_to_sell(context, data):
    list_to_sell = []
    list_hold = context.portfolio.positions.keys()
    if len(list_hold) == 0:
        return list_to_sell
    
    for i in list_hold:
        '''
        close = history(1, '1d', 'close', [i],df = False)[i][0]
        # 跌到MA60卖
        ma60 = count_ma(i, 60, 0)
        if close < ma60:
            list_to_sell.append(i)
            continue
        '''
        # 均线纠缠时，不进行操作
        if is_struggle(count_ma(i,5,1),count_ma(i,10,1),count_ma(i,20,1)):
            continue
        # 空头排列——清仓卖出
        if is_lowest_point(data,i,-1):
            list_to_sell.append(i)
        # 多头排列后死叉——清仓卖出 10 叉 20
        elif is_crossDOWN(data,i,10,20):
            list_to_sell.append(i)
        '''
        if context.portfolio.positions[i].avg_cost *0.95 >= data[i].close:
            #亏损 5% 卖出
            list_to_sell.append(i)
        if context.portfolio.positions[i].avg_cost *1.15 <= data[i].close:
            #赚 10% 卖出
            list_to_sell.append(i)
        '''
    return list_to_sell
# 获得均线卖出信号
# 输入：context（见API文档）
# 输出：list_to_sell为list类型，表示待卖出的股票
def stocks_udma_to_sell(context, data):
    list_to_sell = []
    list_hold = context.portfolio.positions.keys()
    if len(list_hold) == 0:
        return list_to_sell
    for stock in list_hold:
        ma20 = count_ma(stock, 20, 0)
        close2day = history(2, '1d', 'close', [stock],df = False)[stock]
        if is_struggle(close2day[-2],close2day[-1],ma20):
            continue
        if close2day[-2] > ma20 and close2day[-1] < ma20:
            list_to_sell.append(stock)
    return list_to_sell
#8
# 获得卖出信号
# 输入：context（见API文档）, list_to_buy为list类型，代表待买入的股票
# 输出：list_to_sell为list类型，表示待卖出的股票
def stocks_to_sell(context, data, list_to_buy):
    # 对于不需要持仓的股票，全仓卖出
    list_to_sell = []
    list_to_sell = get_clear_stock(context, list_to_buy)
    if g.trade_skill:
        list_to_sell2 = stocks_udma_to_sell(context, data)
        for i in list_to_sell2:
            if i not in list_to_sell:
                list_to_sell.append(i)
    #log.debug(list_to_sell) 
    return list_to_sell

# 平仓，卖出指定持仓
# 平仓成功并全部成交，返回True
# 报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
def close_position(position):
    security = position.security
    order = order_target_value_(security, 0) # 可能会因停牌失败
    if order != None:
        if order.filled > 0 and g.flag_stat:
            # 只要有成交，无论全部成交还是部分成交，则统计盈亏
            g.trade_stat.watch(security, order.filled, position.avg_cost, position.price)
    return False
    
    
# 自定义下单
# 根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target_value）返回即表示报单完成
# 报单成功返回报单（不代表一定会成交），否则返回None
def order_target_value_(security, value):
    if value == 0:
        log.debug("Selling out %s" % (security))
    else:
        log.debug("Order %s to value %f" % (security, value))
        
    # 如果股票停牌，创建报单会失败，order_target_value 返回None
    # 如果股票涨跌停，创建报单会成功，order_target_value 返回Order，但是报单会取消
    # 部成部撤的报单，聚宽状态是已撤，此时成交量>0，可通过成交量判断是否有成交
    return order_target_value(security, value)
    
#9
# 执行卖出操作
# 输入：list_to_sell为list类型，表示待卖出的股票
# 输出：none
def sell_operation(context, list_to_sell):
    for stock_sell in list_to_sell:
        position = context.portfolio.positions[stock_sell]
        close_position(position)


#10
# 执行买入操作
# 输入：context(见API)；list_to_buy为list类型，表示待买入的股票
# 输出：none
def buy_operation(context, list_to_buy):
    for stock_buy in list_to_buy:
        # 为每个持仓股票分配资金
        g.capital_unit=context.portfolio.portfolio_value/g.num_stocks
        # 买入在"待买股票列表"的股票
        order_target_value(stock_buy, g.capital_unit)

'''
================================================================================
每天收盘后
================================================================================
'''
# 每天收盘后做的事情
def after_trading_end(context):
    if g.flag_stat:
        g.trade_stat.report(context)
    return
