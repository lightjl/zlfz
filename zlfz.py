# 《彼得林奇的"成功投资"》
# 回测：2006-1-1到2016-5-31，￥1000000 ，每天

import pandas as pd
import time

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

#1 
#设置策略参数
def set_params():
    g.tc = 1                                 # 调仓天数
    g.num_stocks = 10                        # 每次调仓选取的最大股票数量
    g.stocks=get_index_stocks('000300.XSHG') # 设置沪深300为初始股票池
    g.per = 0.1                              # EPS增长率不低于0.25

#2
#设置中间变量
def set_variables():
    g.t = 0                                  # 记录回测运行的天数
    g.if_trade = False                       # 当天是否交易

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
    if g.t%g.tc==0:
        g.if_trade=True                       # 每g.tc天，调仓一次
        set_slip_fee(context)                 # 设置手续费与手续费
        # 设置可行股票池
        g.feasible_stocks = set_feasible_stocks(g.stocks,context)
    g.t+=1
    
    
#4
# 设置可行股票池：过滤掉当日停牌的股票
# 输入：initial_stocks为list类型,表示初始股票池； context（见API）
# 输出：unsuspened_stocks为list类型，表示当日未停牌的股票池，即：可行股票池
def set_feasible_stocks(initial_stocks,context):
    # 判断初始股票池的股票是否停牌，返回list
    paused_info = []
    current_data = get_current_data()
    for i in initial_stocks:
        paused_info.append(current_data[i].paused)
    df_paused_info = pd.DataFrame({'paused_info':paused_info},index = initial_stocks)
    unsuspened_stocks =list(df_paused_info.index[df_paused_info.paused_info == False])
    
    df_st_info = get_extras('is_st',unsuspened_stocks,start_date=context.current_dt,end_date=context.current_dt)
    df_st_info = df_st_info.T
    df_st_info.rename(columns={df_st_info.columns[0]:'is_st'}, inplace=True)
    unsuspened_stocks = list(df_st_info.index[df_st_info.is_st == False])
    return unsuspened_stocks


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
    if g.if_trade == True:
        # 待买入的g.num_stocks支股票，list类型
        list_can_buy = stocks_to_buy(context)
        # 待卖出的股票，list类型
        list_to_sell = stocks_to_sell(context, list_can_buy)
        # 需买入的股票
        list_to_buy = pick_buy_list(context, list_can_buy)
        # 卖出操作
        sell_operation(list_to_sell)
        # 买入操作
        buy_operation(context, list_to_buy)
    g.if_trade = False
    
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

    year = context.current_dt.year
    list_stock = list(df_Growth_PE_G.loc[:,'code'])
	
    q_PE_G2 = query(valuation.code, valuation.capitalization, cash_flow.pubDate,indicator.eps,\
	            cash_flow.subtotal_operate_cash_inflow, cash_flow.subtotal_operate_cash_outflow,\
	            indicator.inc_operation_profit_year_on_year
                 ).filter(valuation.code.in_(list_stock))

    buy_list_stocks = []
    #K70 	房地产业    J66 货币金融服务
    list_fdc = get_industry_stocks('K70')
    list_hbjr = get_industry_stocks('J66')
    # 查询股票池里股票的EPS,股本(万股),发布财报日期,eps,
    # 经营活动现金流入(元),经营活动现金流出(元)
    # 净利润同比增长率 ==> 每股现金流
    # 处理过去3年的数据
    # 净利润同比增长率 inc_operation_profit_year_on_year 连续3年>= 25
    # 动态市盈率 <= 40
    # 资产负债率 < 0.5
    # 每股现金流 > EPS(去年)
    # 相对强度 xdqd12 >= xdqd1 >=0	
    pe_ration_max = 40
    for i in list_stock:
        # 去掉房地产
        if i in list_fdc:
            continue
        q_PE_G2 = query(valuation.code, valuation.capitalization, cash_flow.pubDate,income.basic_eps, valuation.circulating_market_cap, \
	            cash_flow.subtotal_operate_cash_inflow, cash_flow.subtotal_operate_cash_outflow,\
	            indicator.inc_operation_profit_year_on_year
                 ).filter(valuation.code == i)
        yearP1 = get_fundamentals(q_PE_G2, statDate=str(year-1))
        if yearP1.empty:
            continue
        # 流通市值>500跳过
        #if yearP1.loc[0,'circulating_market_cap'] > 500:
        #    continue
        #log.info(yearP1)
        
        if context.current_dt.strftime("%Y-%m-%d") > yearP1.loc[0,'pubDate']:
            yearL1 = year-1
            yearL2 = year-2
            yearL3 = year-3
            yearL4 = year-4
        else:
            yearL1 = year-2
            yearL2 = year-3
            yearL3 = year-4
            yearL4 = year-5
        yearP1 = get_fundamentals(q_PE_G2, statDate=str(yearL1))
        yearP2 = get_fundamentals(q_PE_G2, statDate=str(yearL2))
        yearP3 = get_fundamentals(q_PE_G2, statDate=str(yearL3))
        yearP4 = get_fundamentals(q_PE_G2, statDate=str(yearL4))
        
        
        if yearP4.empty or yearP3.empty or yearP2.empty or yearP1.empty:
            continue
        
            #
        if yearP1.loc[0, 'basic_eps']*yearP1.loc[0, 'capitalization'] >= (1+g.per)*yearP2.loc[0, 'basic_eps']*yearP2.loc[0, 'capitalization'] and \
             yearP2.loc[0, 'basic_eps']*yearP2.loc[0, 'capitalization'] >= (1+g.per)*yearP3.loc[0, 'basic_eps']*yearP3.loc[0, 'capitalization'] and \
             yearP3.loc[0, 'basic_eps']*yearP3.loc[0, 'capitalization'] >= (1+g.per)*yearP4.loc[0, 'basic_eps']*yearP4.loc[0, 'capitalization']:
            
            #log.info("code=%s, market_cap=%d", i, yearP1.loc[0, 'circulating_market_cap'])
            #log.debug(yearP1)
            #log.debug(yearP2) 
            #log.debug(yearP3)
            #log.debug(yearP4)
            # 动态市盈率 负债合计(元)/负债和股东权益合计
            q_PE_G = query(valuation.pe_ratio, balance.total_liability, balance.total_sheet_owner_equities
                    ).filter(valuation.code == i)
            jbm = get_fundamentals(q_PE_G, statDate=str(year-1))
            
            #满分 1+1+1
            valueOfStock = 0
            # pe_ratio 动态市盈率  负债合计(元)/负债和股东权益合计= 资产负债率 < 0.5
            zcfzl = jbm.loc[0, 'total_liability']/jbm.loc[0, 'total_sheet_owner_equities']
            if jbm.loc[0, 'pe_ratio'] <= pe_ration_max:
                if zcfzl<0.5:
                    valueOfStock = valueOfStock + 1
                #每股现金流 > EPS(去年)
                if yearP1.loc[0, 'subtotal_operate_cash_inflow'] - yearP1.loc[0, 'subtotal_operate_cash_outflow'] > yearP1.loc[0, 'basic_eps']*yearP1.loc[0, 'capitalization']:
                    valueOfStock = valueOfStock + 1
                # 相对强度
                h = history(260, security_list=['000001.XSHG', i])
                dpqd1 = (h['000001.XSHG'][-1]-h['000001.XSHG'][-22])/h['000001.XSHG'][-22]
                ggqd1 = (h[i][-1]-h[i][-22])/h[i][-22]
                dpqd12 = (h['000001.XSHG'][-1]-h['000001.XSHG'][-260])/h['000001.XSHG'][-260]
                ggqd12 = (h[i][-1]-h[i][-260])/h[i][-260]
                xdqd12 = (ggqd12 - dpqd12)/ abs(dpqd12)
                xdqd1 = (ggqd1 - dpqd1)/ abs(dpqd1)
                if xdqd12 >= xdqd1 and xdqd1 >= 0:
                    valueOfStock = valueOfStock + 1
                if valueOfStock >= 2:
                    buy_list_stocks.append(i)
            #log.info(yearP2)
            #log.info(yearP3)
            #log.info("code=%s, zcfzl=%.2f, sylyc=%f, yearL1 = %d, P1=%f, P2=%f, P3=%f", i, zcfzl, jbm.loc[0, 'pe_ratio'], \
            #                yearL1, yearP1.loc[0, 'eps'], yearP2.loc[0, 'eps'], yearP3.loc[0, 'eps'])
    return buy_list_stocks
    
#7
# 获得买入信号
# 输入：context(见API)
# 输出：list_to_buy为list类型,表示待买入的g.num_stocks支股票
def stocks_to_buy(context):
    list_to_buy = []
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    df_PEG = get_PEG(context, get_growth_stock(context, g.feasible_stocks))
    # 将股票按PEG升序排列，返回daraframe类型
    df_sort_PEG = df_PEG.sort(columns=[0], ascending=[1])
    # 将存储有序股票代码index转换成list并取前g.num_stocks个为待买入的股票，返回list
    nummax = min(len(df_PEG.index), g.num_stocks-len(context.portfolio.positions.keys()))
        
    for i in range(nummax):
        if df_sort_PEG.ix[i,0] < 0.75:
            list_to_buy.append(df_sort_PEG.index[i])
    return list_to_buy
    
    
# 已不再具有持有优势的股票
# 输入：context(见API)；stock_list_now为list类型，表示初始持有股, stock_list_buy为list类型，表示可以买入的股票
# 输出：应清仓的股票 list
def get_clear_stock(context, stock_list_now, stock_list_buy):
    # 

    stock_hold = []
    year = context.current_dt.year
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
    log.info(news_ids, "stock_can_buy")
    
    
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
            log.info("code=%s PEG>1.2", i)
            
    return list_to_sell
    
# 获得买入的list_to_buy
# 输入list_can_buy 为list，可以买的队列
# 输出list_to_buy 为list，买入的队列
def pick_buy_list(context, list_can_buy):
    list_to_buy = []
    buy_num = g.num_stocks - len(context.portfolio.positions.keys())
    if buy_num <= 0:
        return list_to_buy
    # 得到一个dataframe：index为股票代码，data为相应的PEG值
    for i in list_can_buy:
        if i not in context.portfolio.positions.keys():
            list_to_buy.append(i)
    return list_to_buy


#8
# 获得卖出信号
# 输入：context（见API文档）, list_to_buy为list类型，代表待买入的股票
# 输出：list_to_sell为list类型，表示待卖出的股票
def stocks_to_sell(context, list_to_buy):
    # 对于不需要持仓的股票，全仓卖出
    list_to_sell = get_clear_stock(context, context.portfolio.positions.keys(), list_to_buy)

    return list_to_sell


#9
# 执行卖出操作
# 输入：list_to_sell为list类型，表示待卖出的股票
# 输出：none
def sell_operation(list_to_sell):
    for stock_sell in list_to_sell:
        order_target_value(stock_sell, 0)


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
# 进行长运算（本策略中不需要）
def after_trading_end(context):
    return
    

