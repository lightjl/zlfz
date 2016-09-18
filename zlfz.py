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
    # 设置移动均线天数
    g.ma_length = 30
    # 设置判断输赢的天数
    g.after_days = 30
    # 赢是几个sigma
    g.win_times_sigma = 1
    # 输是几个sigma
    g.lose_times_sigma = 1
    # 最小数据比例
    g.least_percentage = 0.05
    # 计量输赢时取区间宽度
    g.band_width = 4
    # 交易时止盈线
    g.profit_times_sigma = 1
    # 交易时止损线
    g.loss_times_sigma = 1

# ---代码块2.设置全局变量
def set_variables():
    # 股票的卖出判定
    g.sell_conditions = {}  
    # 记录输赢统计
    g.stock_stats = {}
    # 记录最佳买入区间
    g.stock_best_ranges = {}

#3
#设置回测条件
def set_backtest():
    set_option('use_real_price',True)        # 用真实价格交易
    # 设置回测基准
    set_benchmark('000300.XSHG')
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
    g.security = stocks_can_buy(context)
    log.debug(g.security)
    initiate_statistics(context)
    # 更新输赢统计， 没优化前不用更新，全部重算
    # update_statistics(context)
#4
# 设置可行股票池：过滤掉当日停牌的股票
# 输入：initial_stocks为list类型,表示初始股票池； context（见API）
# 输出：unsuspened_stocks为list类型，表示当日未停牌的股票池，即：可行股票池
def set_feasible_stocks(initial_stocks,context):
    # 判断初始股票池的股票是否停牌，返回list
    unsuspened_stocks =filter_paused_stock(initial_stocks)    
    unst_stocks = filter_st_stock(unsuspened_stocks)
    return unst_stocks

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
    list_to_sell = stocks_to_sell(context, data, g.security)
    # 需买入的股票
    list_to_buy = pick_buy_list(context, data, g.security)
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
    q_PE_G = query(valuation.code, valuation.pe_ratio_lyr, indicator.inc_operation_profit_year_on_year
                 ).filter(valuation.code.in_(stock_list)) 
    # 得到一个dataframe：包含股票代码、市盈率PE、收益增长率G
    # 默认date = context.current_dt的前一天,使用默认值，避免未来函数，不建议修改
    df_PE_G = get_fundamentals(q_PE_G)
    # 筛选出成长股：删除市盈率或收益增长率为负值的股票
    df_Growth_PE_G = df_PE_G[(df_PE_G.pe_ratio_lyr >0)&(df_PE_G.inc_operation_profit_year_on_year >0)]
    # 去除PE或G值为非数字的股票所在行
    df_Growth_PE_G.dropna()
    # 得到一个Series：存放股票的市盈率TTM，即PE值
    Series_PE = df_Growth_PE_G.ix[:,'pe_ratio_lyr']
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
    # 动态市盈率 <= 40
    # 资产负债率 < 0.5
    # 每股现金流 > EPS(去年)
    # 相对强度 xdqd12 >= xdqd1 >=0	
    pe_ration_max = 40
    
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
        #if .loc[0,'circulating_market_cap'] > 500:
        #    continue
        #log.info(yearP1)
        startth = 1
        if context.current_dt.strftime("%Y-%m-%d") < yearP1.loc[0,'pubDate']:
            start_yearth = 0
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
        df_lastyear = yearP[3+startth][yearP[3+startth].code==i]
        flag_cz = True        
        for j in range(3):      # 2011 2012 2013 2014 or 2012 ... 今年2016
            if (1+g.per)*eps[j]*cap[j] > eps[j+1]*cap[j+1]:
                # 增长不达
                flag_cz = False
                break
        # todo
        if flag_cz:
            #log.info("code=%s, market_cap=%d", i, yearP1.loc[0, 'circulating_market_cap'])
            gg_price = get_price(i, start_date=last_year, end_date=last_month+timedelta(1), frequency='daily', fields='close')
            # 动态市盈率 负债合计(元)/负债和股东权益合计
            
            #满分 1+1+1
            scoreOfStock = 0
            # pe_ratio 动态市盈率  负债合计(元)/负债和股东权益合计= 资产负债率 < 0.5
            zcfzl = df_lastyear['total_liability'].values[0]/df_lastyear['total_sheet_owner_equities'].values[0]
            if df_lastyear['pe_ratio'].values[0] <= pe_ration_max:
                if zcfzl<0.5:
                    scoreOfStock = scoreOfStock + 1
                #每股现金流 > EPS(去年)
                if df_lastyear['subtotal_operate_cash_inflow'].values[0] - df_lastyear['subtotal_operate_cash_outflow'].values[0] > df_lastyear['basic_eps'].values[0]*df_lastyear['capitalization'].values[0]*10000:
                    scoreOfStock = scoreOfStock + 1
                # 相对强度
                dpqd1 = (dp_price['close'][last_month]-dp_price['close'][last_2month])/dp_price['close'][last_2month]
                ggqd1 = (gg_price['close'][last_month]-gg_price['close'][last_2month])/gg_price['close'][last_2month]
                dpqd12 = (dp_price['close'][last_month]-dp_price['close'][last_year])/dp_price['close'][last_year]
                ggqd12 = (gg_price['close'][last_month]-gg_price['close'][last_year])/gg_price['close'][last_year]
                xdqd12 = (ggqd12 - dpqd12)/ abs(dpqd12)
                xdqd1 = (ggqd1 - dpqd1)/ abs(dpqd1)
                if xdqd12 >= xdqd1 and xdqd1 >= 0:
                    scoreOfStock = scoreOfStock + 1
                if scoreOfStock >= 2:
                    buy_list_stocks.append(i)
            #log.info(yearP2)
            #log.info(yearP3)
            log.info("code=%s, score=%d zcfzl=%.2f, eps=%f, yearL1 = %d, mgxjl=%.2f", i, scoreOfStock, zcfzl, eps[3], \
            year-5+startth, (df_lastyear['subtotal_operate_cash_inflow'].values[0] - df_lastyear['subtotal_operate_cash_outflow'].values[0])/df_lastyear['capitalization'].values[0]/10000)
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
    # nummax = min(len(df_PEG.index), g.num_stocks-len(context.portfolio.positions.keys()))
        
    for i in range(len(df_sort_PEG.index)):
        if df_sort_PEG.ix[i,0] < 0.75:
            list_can_buy.append(df_sort_PEG.index[i])
    return list_can_buy
    
    
 
'''
================================================================================
均值回归策略
================================================================================
'''

# --代码块4
# 初始化输赢统计
# 无输出
# 更新全局变量一dict的DataFrame，key为股票，内容为偏差倍数和输赢的统计
def initiate_statistics(context):
    # 初始化输赢统计
    for security in g.security:
        '''
        # 股票池动态改变，未确保正确只好浪费下时间
        # 有机会再优化
        if g.stock_stats.has_key(security):
            continue
        '''
        # 上个日期
        previous_date = context.previous_date
        # 获取历史收盘价数据
        prices = get_price(security, start_date='2006-01-01', end_date=previous_date, frequency='daily', fields=['close'], skip_paused = True)['close']
        # 得到偏离倍数和输赢结果的记录
        my_data = collect_data(prices, g.ma_length)
        # 对上面取得的数据进行数量统计
        statistics = compute_statistics(my_data)
        # 将统计结果做成DF
        my_df = pd.DataFrame(statistics, index=['value', 'win', 'even', 'lose'])
        # 转置
        g.stock_stats[security] = my_df.T



# ---代码块5. 
# 收集输赢数据
# 输入一list价格和位置i
# 返回一list的pairs，是i天之后每一天的价格偏离倍数和之后的输赢结果。
def collect_data(prices, i):
    # 初始化记录
    my_data = []
    # 当i的位加后置天数没有超过数据长度
    while i + g.after_days < len(prices):
        # 取过去ma长度天数的数据
        range_data = prices[i-g.ma_length: i]
        # 算均线
        ma = mean(range_data)
        # 算标准差
        sigma = std(range_data)
        # 算偏差倍数*10，乘十是因为整数更方便操作
        difference_times_sigma =int(( ((prices[i] - ma) / sigma) // 0.1) )
        # 如果-10< 偏离倍数 <= -1，因为小于-10的也太异常了，因此也不要
        if -100< difference_times_sigma <= -10:
            # 计算输赢结果
            result = win_or_lose(prices.iloc[i], prices[i+1: i+g.after_days+1], sigma)
            # 将偏离倍数和输赢结果记录下来
            my_data.append((difference_times_sigma, result))
        # i++
        i += 1
    return(my_data)

# ---代码块6. 
# 进行数量统计
# 输入一list的pairs，每一个pair是偏离倍数和输赢结果
# 返回一dict,key是偏离倍数，内容是‘输’‘赢’‘平’分别出现多少次
def compute_statistics(my_data):
    # 创建字典进行统计
    statistics = {}
    # 数据还没空的时候
    for pair in my_data:
        # 输赢是怎么样的呀
        result = pair[1]
        # 偏离倍数呢
        value = pair[0]
        # 如果这个偏离倍数还没出现过
        if value not in statistics:
            # 那就记下来！
            statistics[value] = {'lose': 0, 'even': 0, 'win':0}
        # 输赢结果的统计加一
        statistics[value][result] += 1
    return(statistics)

# --代码块7.判断输赢
# 输入价格、一Series的价格和sigma，返回是赢还是输还是平
def win_or_lose(price, my_list, sigma):
    # 设上限
    upper_bound = price + g.win_times_sigma * sigma
    # 设下限
    lower_bound = price - g.lose_times_sigma * sigma
    # 未来几天里
    for future_price in my_list:
        # 碰到上线了
        if future_price >= upper_bound:
            # 赢
            return('win')
        # 碰到下线了
        if future_price <= lower_bound:
            # 输
            return('lose')
    # 要不就是平
    return('even')
# ---代码块9.
# 更新输赢统计
# 无输出
# 更新全局变量的偏离倍数和输赢统计DF
def update_statistics(context):
    for security in g.security:
        # 取价格
        prices = attribute_history(security, 1+g.ma_length+g.after_days, '1d', 'close', skip_paused = True)['close'] 
        # 上一交易日是否停牌
        paused = attribute_history(security, 1, '1d', 'paused')['paused'].iloc[0]
        # 上个交易日没停牌的话就更新
        if paused == 0:
            # 算sigma的日子
            past_prices = prices[0:g.ma_length]
            # 对应的当天
            current_price = prices[g.ma_length]
            # 算输赢的日子
            future_prices = prices[g.ma_length + 1: ]
            # 算ma
            ma = mean(past_prices)
            # 算sigma
            sigma = std(past_prices)
            # 计算和ma差几个sigma 
            difference_times_sigma = int((current_price - ma)/sigma // 0.1)
            # 上线
            upper_bound = current_price + g.win_times_sigma * sigma
            # 下限
            lower_bound = current_price - g.lose_times_sigma * sigma
            # 判断过后几天的输赢
            result = win_or_lose(current_price, future_prices, sigma)
            # 把DF转成dict进行操作
            my_dict = g.stock_stats[security].T.to_dict()
            # 在合理区间里的话
            if -100 < difference_times_sigma <= -10:
                # 如果dict里有这个倍数了
                if difference_times_sigma in my_dict:
                    # 直接更新输赢
                    my_dict[difference_times_sigma][result] += 1 
                # 如果没有
                else:
                    # 加进去
                    my_dict[difference_times_sigma] = {'value': difference_times_sigma, 'win' : 0, 'even' : 0, 'lose' : 0}
                    # 更新输赢
                    my_dict[difference_times_sigma][result] = 1
            # 更新全局变量
            g.stock_stats[security] = pd.DataFrame(my_dict, index=['win', 'even', 'lose']).T
            
# --代码块10.
# 判断最佳区间
# 无输出
# 返回一dict,key为股票，值为最佳买入区域DF
def get_best_ranges():
    stock_best_ranges = {}
    for security in g.stock_stats:
        statistics = g.stock_stats[security]
        # 获取偏离倍数
        values = statistics.index
        # 输数
        loses = statistics['lose']
        # 赢数
        wins = statistics['win']
        # 平数
        evens = statistics['even']
        # 总数
        num_data = sum(wins) + sum(loses) + sum(evens)
        mydata = []
        # 在所有位置不会溢出的位置
        for n in range(min(values), max(values) - (g.band_width-1)):
            # 取在n和（n+宽度）之间的DF行
            stat_in_range = statistics[(values >= n) & (values <= n+g.band_width-1)]
            # 赢除输（这里输+1，因为可能输=0）
            ratio = float(sum(stat_in_range['win'])) / float((sum(stat_in_range['lose']) + 1))
            # 这区间数据总量
            range_data = float(sum(stat_in_range['win']) + sum(stat_in_range['lose']) + sum(stat_in_range['even']))
            # 如果数据量超过预设的临界值
            if range_data / num_data >= g.least_percentage:
                # 记录区间的输赢比
                mydata.append({'low': n, 'high': n+g.band_width, 'ratio': ratio})
        # 区间统计转换成DF
        data_table = pd.DataFrame(mydata)
        # 按输赢比排序
        sorted_table = data_table.sort('ratio', ascending = False)
        # 取第一行
        stock_best_range = sorted_table.iloc[0]
        stock_best_ranges[security] = stock_best_range
    # 输出结果
    return(stock_best_ranges)
'''
================================================================================
均值回归策略
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
        # 判断最好的买入区间
        stock_best_ranges = get_best_ranges()
        for stock in list_can_buy:
            if stock in context.portfolio.positions.keys():
                continue
            stock_best_range = stock_best_ranges[stock]
            # 看现价
            current_price = attribute_history(stock,1, '1d', 'close').iloc[0,0]
            # 取倍数区间低点
            low = float(stock_best_range['low'])
            # 取倍数区间高点
            high = float(stock_best_range['high'])
            # 取赢率
            ratio = float(stock_best_range['ratio'])
            # 获取历史收盘价
            h = attribute_history(stock, g.ma_length, '1d', ['close'], skip_paused=True)['close']
            # 计算均线
            ma = mean(h)
            # 计算标准差
            sigma  = std(h)
            # 算现价的偏离倍数
            times_sigma = (current_price - ma) / sigma
            # 如果在该买的区间里
            if low <= 10 * times_sigma and 10 *times_sigma <= high:
                list_to_buy.append(stock)
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
def stocks_jzhg_to_sell(context, data):
    to_sell = []
    # 对于仓内所有股票
    for security in context.portfolio.positions:
        # 取现价
        current_price = history(1, '1m', 'close', security).iloc[0,0]
        # 查卖出条件
        conditions = g.sell_conditions[security]
        # 看看是不是该卖了
        if current_price >= conditions['high'] or current_price <= conditions['low'] or conditions['days'] <= 0:
            # 加入卖出信号，确保没有重复
            to_sell.append(security)
        # 如果不需要卖
        else:
            # 日数减1
            g.sell_conditions[security]['days'] -= 1
    return(to_sell)
#8
# 获得卖出信号
# 输入：context（见API文档）, list_to_buy为list类型，代表待买入的股票
# 输出：list_to_sell为list类型，表示待卖出的股票
def stocks_to_sell(context, data, list_to_buy):
    # 对于不需要持仓的股票，全仓卖出
    list_to_sell = []
    list_to_sell = get_clear_stock(context, list_to_buy)
    if g.trade_skill:
        list_to_sell2 = stocks_jzhg_to_sell(context, data)
        for i in list_to_sell2:
            if i not in list_to_sell:
                list_to_sell.append(i)
    #log.debug(list_to_sell) 
    return list_to_sell

# 平仓，卖出指定持仓
# 平仓成功并全部成交，返回True
# 报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
def close_position(context, position):
    security = position.security
    order = order_target_value_(security, 0) # 可能会因停牌失败
    if order != None:
        if order.filled > 0 and g.flag_stat:
            # 只要有成交，无论全部成交还是部分成交，则统计盈亏
            g.trade_stat.watch(security, order.filled, position.avg_cost, position.price)
    
    if security in context.portfolio.positions:
        # 把天数清零
        g.sell_conditions[security]['days'] = 0
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
        close_position(context, position)


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
        if stock_buy in context.portfolio.positions:
            # 看现价
            current_price = attribute_history(stock_buy,1, '1d', 'close').iloc[0,0]
            # 获取历史收盘价
            h = attribute_history(stock_buy, g.ma_length, '1d', ['close'], skip_paused=True)['close']
            # 计算均线
            ma = mean(h)
            # 计算标准差
            sigma  = std(h)
            # 止损线
            low = current_price - g.loss_times_sigma * sigma
            # 止盈线
            high = current_price + g.profit_times_sigma * sigma
            # 在全局变量中记录卖出条件
            g.sell_conditions[stock_buy] = {'high': high, 'low': low, 'days': g.after_days-1}

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
    

