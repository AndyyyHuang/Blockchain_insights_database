use DEX;
# 当我们拥有了实时的链上交易数据后，我们就可以对链上交易数据进行分析，从而寻找交易机会。

# 功能一 寻找inside trader。由于在区块链上每个账户的地址和其账户信息（交易记录、持仓）都是公开信息。
# 因此当某些币因为某些特定的大事件、新闻发生而产生价格的剧烈波动后，我们可以去搜寻在该时间点前哪些账户地址大笔的买入或者卖出这种币，这些账户可能就是潜在的内幕交易者持有的账户。
delimiter //
create procedure find_inside_trader(inter_val int, time_point date, token char(10), limit_num int, side char(10))
deterministic
begin
    select distinct address, sum(amount) as amount_sum from transaction
          where transaction.side = side and transaction.token = token and transaction.time_stamp < time_point
            and transaction.time_stamp > date_sub(time_point, interval inter_val day)
          group by address order by amount_sum DESC
    limit limit_num;
end;
delimiter //
call find_inside_trader(2, '2022-03-02', 'MBOX', 10, 'buy');


# 功能二 计算每个用户对应币种的持仓成本。
create view position_cost as
    select address, token, sum(case when side = 'buy' then 1*amount
        else -1*amount end) as position,
        sum(case when side = 'buy' then 1*amount*transaction.price
        else -1*amount*transaction.price end) / sum(case when side = 'buy' then 1*amount
        else -1*amount end) as average_cost
        from transaction group by address, token;


# 功能三 汇总每个用户每日买卖方向的交易次数、交易额
create view account_daily_buy_trades as
select distinct address, date_format(time_stamp, '%Y-%m-%d') as days,
       sum(price*amount) over (partition by address, date_format(time_stamp, '%Y-%m-%d')) as buy_volumn,
       count(transaction_id) over (partition by address, date_format(time_stamp, '%Y-%m-%d')) as buy_num
from transaction where side = 'buy' order by days;

create view account_daily_sell_trades as
select distinct address, date_format(time_stamp, '%Y-%m-%d') as days,
       sum(price*amount) over (partition by address, date_format(time_stamp, '%Y-%m-%d')) as sell_volumn,
       count(transaction_id) over (partition by address, date_format(time_stamp, '%Y-%m-%d')) as sell_num
from transaction where side = 'sell' order by days;

# 功能四 汇总每个币每日的买卖力量变化。
create view token_money_flow as
    select distinct token, date_format(time_stamp, '%Y-%m-%d') as days,
        sum(case when side = 'buy' then amount * price end) over (partition by token, date_format(time_stamp, '%Y-%m-%d')) as net_inflow,
        sum(case when side = 'sell' then amount * price end) over (partition by token, date_format(time_stamp, '%Y-%m-%d')) as net_outflow
    from transaction order by days, token;

# 功能五 根据交易记录计算每个帐户的盈亏
create view accounts_profit as
    select distinct address, sum(-1 * a.position * a.average_cost + a.position * b.price) over (partition by address)profit
    from position_cost a join token b on a.token = b.token order by profit DESC;

# 功能六 寻找巨鲸地址
delimiter //
create procedure find_rich_address(threshold int)
deterministic
begin
select distinct address, sum(a.position * b.price) as total_value
    from account a cross join token b on a.token = b.token group by address having total_value > threshold order by total_value DESC;
end;
delimiter //

call find_rich_address(300000)


