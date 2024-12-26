import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from utils import handle_date_time

def plot_equity_and_drawdown_filled(df):
    """
    Function to plot the equity curve and drawdown plot with filled regions.
    :param df: DataFrame containing the 'datetime', 'capital', and 'drawdown_percentage' columns.
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    # Plotting the equity curve on the first subplot
    ax1.plot(df['datetime'], df['capital'], label='Equity Curve', color='blue')
    ax1.set_ylabel('Capital')
    ax1.set_title('Equity Curve')
    ax1.legend()

    # Plotting the drawdown curve on the second subplot with filled regions
    ax2.fill_between(df['datetime'], df['drawdown_percentage'], 0, 
                     where=(df['drawdown_percentage'] < 0), color='red', alpha=0.5, label="Drawdown (%)")
    ax2.axhline(0, color='black', linewidth=1, linestyle='--')  # Add a 0% reference line
    ax2.set_ylabel('Drawdown Percentage (%)')
    ax2.set_title('Drawdown Plot with Filled Regions')
    ax2.legend()

    # Add common x-axis label
    ax2.set_xlabel('Date Time')

    # Improve layout
    fig.tight_layout()

    # Show the plot
    plt.show()


def calculate_max_drawdown(df):
    """
    Function to calculate maximum drawdown percentage.
    :param df: DataFrame containing the 'capital' column.
    :return: Maximum drawdown percentage.
    """
    # Step 1: Calculate the cumulative maximum capital
    df['cumulative_max'] = df['capital'].cummax()

    # Step 2: Calculate the drawdown percentage
    df['drawdown_percentage'] = (df['capital'] - df['cumulative_max']) / df['cumulative_max'] * 100

    # Step 3: Find the maximum drawdown percentage
    max_drawdown_percentage = df['drawdown_percentage'].min()

    return max_drawdown_percentage


def calculate_average_holding_duration(df: pd.DataFrame, date_column: str) -> pd.Timedelta:
    
    # Convert the specified date column to datetime
    # df[date_column] = pd.to_datetime(df[date_column])
    df[date_column] = df[date_column].map(handle_date_time)

    holding_duration = []
    for i in range(0, len(df)-1, 2):
        holding_duration.append(df.loc[i+1, 'datetime'] - df.loc[i, 'datetime'])

    # Calculate the average holding duration
    average_holding_duration = np.mean(holding_duration)
    max_holding_duration = np.max(holding_duration)

    return average_holding_duration, max_holding_duration


def compute_metrics(signals: pd.DataFrame, plot: bool = False, leverage: int = 1, slippage: float = 0.0015, capital: float = 1000):
    signals['returns'] = 0.0
    signals['pnl'] = np.nan
    total_fee = 0
    signals['capital'] = 1000.0
    initial_capital = capital
    gross_profit = 0
    gross_loss = 0
    status = 0
    entry_price = 0
    trades=0
    total_long_trades = 0
    total_short_trades = 0


    for i in range(len(signals)):
        #capital=1000
        signal = signals.loc[i, 'signals']
        if status!=0:
            if status == 1:
                if signal == 0:
                    continue
                # square off the current position
                exit_price = signals.loc[i, 'close']
                signals.loc[i, 'returns'] = ((exit_price - entry_price)/entry_price)*status*leverage 
                signals.loc[i, 'pnl'] = capital*((exit_price-entry_price)/entry_price)*status*leverage
                total_fee += capital*slippage
                capital -= capital*slippage
                capital+=signals.loc[i, 'pnl']
                signals.loc[i, 'capital']=capital
                if signals.loc[i, 'pnl']>0:
                    gross_profit+=(capital - (signals.loc[i-1, 'capital'] if i>0 else 0))
                if signals.loc[i, 'pnl']<0:
                    gross_loss+=(capital - (signals.loc[i-1, 'capital'] if i>0 else 0))
                
                if signal == -1:            # status= 0, do nothing after squaring off
                    status = 0
                elif signal == -2:          # status = -1, go short after squaring off
                    status = -1
                    entry_price = signals.loc[i, 'close']
                    total_short_trades+=1
                elif signal == 1:           # status = 1, go long after squaring off
                    status = 1
                    entry_price = signals.loc[i, 'close']
                    total_long_trades+=1
                trades+=1
                

            elif status == -1:
                if signal == 0:
                    continue
                
                #square off the short position
                exit_price = signals.loc[i, 'close']
                signals.loc[i, 'returns'] = ((exit_price - entry_price)/entry_price)*status*leverage 
                signals.loc[i, 'pnl'] = capital*((exit_price-entry_price)/entry_price)*status*leverage
                total_fee += capital*slippage
                capital -= capital*slippage
                capital+=signals.loc[i, 'pnl']
                signals.loc[i, 'capital']=capital
                if signals.loc[i, 'pnl']>0:
                    gross_profit+=(capital - (signals.loc[i-1, 'capital'] if i>0 else 0))
                if signals.loc[i, 'pnl']<0:
                    gross_loss+=(capital - (signals.loc[i-1, 'capital'] if i>0 else 0))


                if signal == 1:            # status= 0, do nothing after squaring off
                    status = 0
                elif signal == 2:          # status = 1, go long after squaring off
                    status = 1
                    entry_price = signals.loc[i, 'close']
                    total_long_trades+=1
                elif signal == -1:           # status = -1, go short after squaring off
                    status = -1
                    entry_price = signals.loc[i, 'close']
                    total_short_trades+=1
                
                trades+=1

        elif status == 0:
            if signal == 0:
                continue

            entry_price = signals.loc[i, 'close']
            if signal == 1:
                status = 1
                total_long_trades+=1
            elif signal ==-1:
                status = -1
                total_short_trades+=1
            signals.loc[i, 'capital']=capital

    profit_percent = signals[signals['pnl']>0]['pnl'].count()/signals[signals['pnl']!=np.nan]['pnl'].count()
    #max_drawdown = calculate_max_drawdown(signals['capital'].values)
    max_drawdown = calculate_max_drawdown(signals)
    win_rate = (signals[(signals['pnl'] > 0) & (pd.notna(signals['pnl']))]['pnl'].count()/len(signals[pd.notna(signals['pnl'])]))*100
    loss_rate = (signals[(signals['pnl'] < 0) & (pd.notna(signals['pnl']))]['pnl'].count()/len(signals[pd.notna(signals['pnl'])]))*100
    net_profit = (signals.loc[len(signals) - 1, 'capital'] - initial_capital)
    avg_winning_trade = signals[signals['pnl']>0]['pnl'].sum()/signals[signals['pnl']>0]['pnl'].count()
    avg_losing_trade = signals[signals['pnl']<0]['pnl'].sum()/signals[signals['pnl']<0]['pnl'].count()
    buy_and_hold_return = ((signals.loc[len(signals) - 1, 'close']-signals.loc[0, 'close'])/signals.loc[0, 'close'])*initial_capital - initial_capital*slippage
    largest_losing_trade = signals[signals['pnl']<0]['pnl'].min()
    largest_winning_trade = signals[signals['pnl']>0]['pnl'].max()

    avg_returns = signals[signals['returns']!=0]['returns'].mean()
    returns_dev = signals[signals['returns']!=0]['returns'].std()
    sharpe_ratio = (avg_returns/returns_dev)*np.sqrt(365)

    neg_returns = signals[signals['returns']<0]['returns'].std()
    sortino_ratio = (avg_returns/neg_returns)*np.sqrt(365)

    max_pnl = signals['pnl'].max()
    min_pnl = signals['pnl'].min()

    avg_holding_duration, maximum_holding_duration = calculate_average_holding_duration(signals, 'datetime')
    min_portfolio_balance = signals['capital'].min()
    max_portfolio_balance = signals['capital'].max()
    final_balance = signals.loc[len(signals) - 1, 'capital']


    metrics_dict = {'final_balance':final_balance,'gross_profit': gross_profit, 'gross_loss': gross_loss, 'net_profit': net_profit, 'total_long_trades': total_long_trades, 'total_short_trades': total_short_trades,
                  'win_rate': win_rate, 'loss_rate': loss_rate, 'avg_winning_trade': avg_winning_trade, 'avg_losing_trade': avg_losing_trade, 'buy_and_hold_return': buy_and_hold_return,
                  'largest_losing_trade': largest_losing_trade, 'largest_winning_trade': largest_winning_trade, 'max_dd': max_drawdown, 'sharpe_ratio': sharpe_ratio, 'sortino_ratio': sortino_ratio,
                  'average_holding_duration': avg_holding_duration, 'maximum_holding_duration': maximum_holding_duration, 'maximum_pnl': max_pnl, 'minimum_pnl': min_pnl,
                  'min_portfolio_balance': min_portfolio_balance, 'max_portfolio_balance': max_portfolio_balance, 'num_of_trades': trades, 'total_fee': total_fee
                  }
    
    metrics_df = pd.Series(metrics_dict)
    if plot:
        plot_equity_and_drawdown_filled(signals)
    return metrics_df
       