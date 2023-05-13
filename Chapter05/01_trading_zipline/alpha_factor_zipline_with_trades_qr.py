# Copyright 2023 QuantRocket LLC - All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pandas as pd
from pytz import UTC
import zipline.api as algo
from zipline.pipeline import Pipeline
from zipline.pipeline.factors import AverageDollarVolume, Returns
# from zipline.finance.execution import MarketOrder
from zipline.pipeline import Pipeline, CustomFactor

from zipline.api import (date_rules, time_rules, order_target_percent,
                         get_open_orders)
# from pyfolio.utils import extract_rets_pos_txn_from_zipline
# from zipline import run_algorithm


# Settings
MONTH = 21
YEAR = 12 * MONTH
N_LONGS = 200
N_SHORTS = 0
VOL_SCREEN = 1000

start = pd.Timestamp('2010-01-01', tz=UTC)
end = pd.Timestamp('2018-01-01', tz=UTC)
capital_base = 1e7

class MeanReversion(CustomFactor):
    """Compute ratio of latest monthly return to 12m average,
       normalized by std dev of monthly returns"""
    inputs = [Returns(window_length=MONTH)]
    window_length = YEAR

    def compute(self, today, assets, out, monthly_returns):
        df = pd.DataFrame(monthly_returns)
        out[:] = df.iloc[-1].sub(df.mean()).div(df.std())


def compute_factors():
    """Create factor pipeline incl. mean reversion,
        filtered by 30d Dollar Volume; capture factor ranks"""
    mean_reversion = MeanReversion()
    dollar_volume = AverageDollarVolume(window_length=30)
    return Pipeline(columns={'longs'  : mean_reversion.bottom(N_LONGS),
                             'shorts' : mean_reversion.top(N_SHORTS),
                             'ranking': mean_reversion.rank(ascending=False)},
                    screen=dollar_volume.top(VOL_SCREEN))


def exec_trades(data, assets, target_percent):
    """Place orders for assets using target portfolio percentage"""
    for asset in assets:
        if data.can_trade(asset) and not get_open_orders(asset):
            order_target_percent(asset, target_percent)


def rebalance(context, data):
    """Compute long, short and obsolete holdings; place trade orders"""
    factor_data = context.factor_data
    assets = factor_data.index

    longs = assets[factor_data.longs]
    shorts = assets[factor_data.shorts]
    divest = context.portfolio.positions.keys() - longs.union(shorts)

    exec_trades(data, assets=divest, target_percent=0)
    exec_trades(data, assets=longs, target_percent=1 / N_LONGS if N_LONGS else 0)
    exec_trades(data, assets=shorts, target_percent=-1 / N_SHORTS if N_SHORTS else 0)



def initialize(context: algo.Context):
    """Setup: register pipeline, schedule rebalancing,
        and set trading params"""
        
    algo.set_benchmark(algo.symbol('SPY'))
        
    algo.attach_pipeline(compute_factors(), 'factor_pipeline')
    algo.schedule_function(rebalance,
                      date_rules.week_start(),
                      time_rules.market_open()
                      )
                    #   ,
                    #   calendar=calendars.US_EQUITIES)

    algo.set_commission(us_equities=algo.commission.PerShare(cost=0.00075, min_trade_cost=.01))
    algo.set_slippage(us_equities=algo.slippage.VolumeShareSlippage(volume_limit=0.0025, price_impact=0.01))

    
def before_trading_start(context: algo.Context, data: algo.BarData):
    """Run factor pipeline"""
    context.factor_data = algo.pipeline_output('factor_pipeline')
    # assets = context.factor_data.index
    # record(prices=data.current(assets, 'price'))

# backtest = run_algorithm(start=start,
#                          end=end,
#                          initialize=initialize,
#                          before_trading_start=before_trading_start,
#                          capital_base=capital_base)


# # depending on the pyfolio version, gross_leverage may also be returned
# returns, positions, transactions = extract_rets_pos_txn_from_zipline(backtest)


# print(returns.head())
# print(returns.info())

# # print(gross_lev.info())
# # print(gross_lev.head())

# print(positions.info())
# print(positions.head())

# print(transactions.info())
# print(transactions.head())