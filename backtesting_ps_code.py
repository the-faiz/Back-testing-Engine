import pandas as pd
from easydict import EasyDict
from datetime import datetime
from utils import get_cfg, load_high_low, adjust, generate_csv, trade_log, tpsl, convert_to_open_timings, handle_date_time
from strategy import BaseStrategy


def generate_signals(
    strategy: BaseStrategy,
    glob: EasyDict,
    high_csv: pd.DataFrame,
    low_csv: pd.DataFrame,
    trade_sheet: pd.DataFrame,
    signal_csv: pd.DataFrame,
    low_time: int = 3,
    high_time: int = 1440,
    margin: float = 0.02,
    leverage: int = 1,
    trailing: bool = False,
    slippage: float = 0.0015,
    capital: float = 1000,
    entry_date: datetime = None,
    exit_date: datetime = None,
):
    """Signal Generation for the backtesting or live trading

    Args:
        strategy (BaseStrategy): Strategy to be used for generating the signals
        glob (EasyDict): Global variables to store the status of the trade
        high_csv (pd.DataFrame): High timeframe data
        low_csv (pd.DataFrame): Low timeframe data
        trade_sheet (pd.DataFrame): Trade Book to log the trades
        signal_csv (pd.DataFrame): Signal file to log the generated signals
        margin (float, optional): Margin for the trade. Defaults to 0.02.
        leverage (int, optional): Leverage for the trade. Defaults to 1.
        trailing (bool, optional): Trailing stop loss. Defaults to False.
        slippage (float, optional): Slippage for the trade. Defaults to 0.0015.
        capital (float, optional): Initial Capital for the trade. Defaults to 1000.
        exit_date (datetime, optional): Exit date for the trade. Defaults to None.
        entry_date (datetime, optional): Entry date for the trade. Defaults to None.
    """

    entry_index = 0
    exit_index = len(high_csv) - 1
    if entry_date:
        entry_index = high_csv[
            pd.to_datetime(high_csv["datetime"]) >= entry_date
        ].index[0]
    if exit_date:
        if pd.to_datetime(high_csv["datetime"].iloc[-1]) <= exit_date:
            exit_index = len(high_csv) - 1
        else:
            exit_index = (
                high_csv[pd.to_datetime(high_csv["datetime"]) > exit_date].index[0] - 1
            )


    open_time_low_pointer = 0
    future_time_diff = 15
    low_pointer = 1
    pnl = 0
    for i in range(entry_index, exit_index):
        low_pointer = adjust(low_pointer, i, low_csv, high_csv)
        # If you are currently in a position check for tpsl
        if glob.status != 0:
            hit, ind = tpsl(low_pointer,i, low_csv,high_csv, margin, leverage, glob, trailing)
            date_time = low_csv["datetime"].iloc[ind]
            exit_price = low_csv["close"].iloc[ind]
            if hit != 0:
                pnl = (
                    capital
                    * ((exit_price - glob.entry_price) / glob.entry_price)
                    * glob.status
                    * leverage
                )
                p = (
                    ((exit_price - glob.entry_price) / glob.entry_price)
                    * glob.status
                    * leverage
                )
                glob.total_fee += capital * slippage
                capital -= capital * slippage
                capital += pnl
                signal = -1 * glob.status
                glob.status = 0
                if pnl > 0:
                    trade_log(
                        date_time,
                        exit_price,
                        capital,
                        signal,
                        glob.status,
                        "TP",
                        p,
                        0,
                        trade_sheet,
                    )
                elif margin / leverage > glob.sl:
                    trade_log(
                        date_time,
                        exit_price,
                        capital,
                        signal,
                        glob.status,
                        "SL",
                        p,
                        0,
                        trade_sheet,
                    )
                else:
                    trade_log(
                        date_time,
                        exit_price,
                        capital,
                        signal,
                        glob.status,
                        "Margin",
                        p,
                        0,
                        trade_sheet,
                    )
                generate_csv(ind, 0, signal, low_csv, high_csv, signal_csv, "tpsl",(low_csv.loc[ind,"datetime"]))
                glob.trades += 1
                continue
        date_time = high_csv["datetime"].iloc[i]
        exit_price = high_csv["close"].iloc[i]
        pnl = (
            capital
            * ((exit_price - glob.entry_price) / glob.entry_price)
            * glob.status
            * leverage
        )
        p = (
            ((exit_price - glob.entry_price) / glob.entry_price)
            * glob.status
            * leverage
        )
        if glob.status == 1:
            if strategy.check_short_entry(i):
                current_time = handle_date_time(high_csv["datetime"].iloc[i])
                open_time_flag , time_to_be_noted, open_time_low_pointer = convert_to_open_timings(current_time, low_csv,open_time_low_pointer, low_time,high_time, future_time_diff)
                if open_time_flag == 0:
                    continue
                glob.total_fee += capital * slippage
                capital -= capital * slippage
                capital += pnl
                glob.status = -1
                signal = -2
                stop_loss = glob.entry_price - glob.entry_price * glob.status * glob.sl
                trade_log(
                    time_to_be_noted,
                    high_csv["close"].iloc[i],
                    capital,
                    signal,
                    glob.status,
                    "Market",
                    p,
                    stop_loss,
                    trade_sheet,
                )
                print("=>Short at ",date_time)
                generate_csv(i, 1, signal, low_csv, high_csv, signal_csv,"market",time_to_be_noted)
                glob.trades += 1
            if strategy.check_long_exit(i):
                current_time = handle_date_time(high_csv["datetime"].iloc[i])
                open_time_flag , time_to_be_noted, open_time_low_pointer = convert_to_open_timings(current_time, low_csv,open_time_low_pointer, low_time,high_time, future_time_diff)
                if open_time_flag == 0:
                    continue
                glob.total_fee += capital * slippage
                capital -= capital * slippage
                capital += pnl
                glob.status = 0
                signal = -1
                trade_log(
                    time_to_be_noted,
                    high_csv["close"].iloc[i],
                    capital,
                    signal,
                    glob.status,
                    "Market",
                    p,
                    0,
                    trade_sheet,
                )
                generate_csv(i, 1, signal, low_csv, high_csv, signal_csv,"market",time_to_be_noted)
                glob.trades += 1

        elif glob.status == -1:
            if strategy.check_long_entry(i):
                current_time = handle_date_time(high_csv["datetime"].iloc[i])
                open_time_flag , time_to_be_noted, open_time_low_pointer = convert_to_open_timings(current_time, low_csv,open_time_low_pointer, low_time,high_time, future_time_diff)
                if open_time_flag == 0:
                    continue
                glob.total_fee += capital * slippage
                capital -= capital * slippage
                capital += pnl
                glob.status = 1
                signal = 2
                stop_loss = glob.entry_price - glob.entry_price * glob.status * glob.sl
                trade_log(
                    time_to_be_noted,
                    high_csv["close"].iloc[i],
                    capital,
                    signal,
                    glob.status,
                    "Market",
                    p,
                    stop_loss,
                    trade_sheet,
                )
                # print("=>Long at ",date_time)
                generate_csv(i, 1, signal, low_csv, high_csv, signal_csv,"market",time_to_be_noted)
                glob.trades += 1
            if strategy.check_short_exit(i):
                current_time = handle_date_time(high_csv["datetime"].iloc[i])
                open_time_flag , time_to_be_noted, open_time_low_pointer = convert_to_open_timings(current_time, low_csv,open_time_low_pointer, low_time,high_time, future_time_diff)
                if open_time_flag == 0:
                    continue
                glob.total_fee += capital * slippage
                capital -= capital * slippage
                capital += pnl
                glob.status = 0
                signal = 1
                trade_log(
                    time_to_be_noted,
                    high_csv["close"].iloc[i],
                    capital,
                    signal,
                    glob.status,
                    "Market",
                    p,
                    0,
                    trade_sheet,
                )
                generate_csv(i, 1, signal, low_csv, high_csv, signal_csv,"market",time_to_be_noted)
                glob.trades += 1

        elif glob.status == 0:
            if strategy.check_long_entry(i):
                current_time = handle_date_time(high_csv["datetime"].iloc[i])
                open_time_flag , time_to_be_noted, open_time_low_pointer = convert_to_open_timings(current_time, low_csv,open_time_low_pointer, low_time,high_time, future_time_diff)
                if open_time_flag == 0:
                    continue
                glob.status = 1
                signal = 1
                stop_loss = glob.entry_price - glob.entry_price * glob.status * glob.sl
                trade_log(
                    time_to_be_noted,
                    high_csv["close"].iloc[i],
                    capital,
                    signal,
                    glob.status,
                    "Market",
                    0,
                    stop_loss,
                    trade_sheet,
                )
                print("=>Long at ",date_time)
                generate_csv(i, 1, signal, low_csv, high_csv, signal_csv,"market",time_to_be_noted)

            elif strategy.check_short_entry(i):
                current_time = handle_date_time(high_csv["datetime"].iloc[i])
                open_time_flag , time_to_be_noted, open_time_low_pointer = convert_to_open_timings(current_time, low_csv,open_time_low_pointer, low_time,high_time, future_time_diff)
                if open_time_flag == 0:
                    continue
                glob.status = -1
                signal = -1
                stop_loss = glob.entry_price - glob.entry_price * glob.status * glob.sl
                trade_log(
                    time_to_be_noted,
                    high_csv["close"].iloc[i],
                    capital,
                    signal,
                    glob.status,
                    "Market",
                    0,
                    stop_loss,
                    trade_sheet,
                )
                print("=>Short at ",date_time)
                generate_csv(i, 1, signal, low_csv, high_csv, signal_csv,"market",time_to_be_noted)

    if glob.status != 0:
        current_time = handle_date_time(high_csv["datetime"].iloc[i])
        open_time_flag , time_to_be_noted, open_time_low_pointer = convert_to_open_timings(current_time, low_csv,open_time_low_pointer, low_time,high_time, future_time_diff)
        if open_time_flag == 0:
            time_to_be_noted = high_csv["datetime"].iloc[exit_index]

        print("Squaring off the position due to end of trade")
        pnl = (
            capital
            * (
                (high_csv["close"].iloc[exit_index] - glob.entry_price)
                / glob.entry_price
            )
            * glob.status
            * leverage
        )
        glob.total_fee += capital * slippage
        capital -= capital * slippage
        capital += pnl
        p = (
            ((high_csv["close"].iloc[exit_index] - glob.entry_price) / glob.entry_price)
            * glob.status
            * leverage
        )
        signal = -1 * glob.status
        glob.status = 0
        trade_log(
            time_to_be_noted,
            high_csv["close"].iloc[exit_index],
            capital,
            signal,
            glob.status,
            "Market",
            p,
            0,
            trade_sheet,
        )
        generate_csv(exit_index, 1, signal, low_csv, high_csv, signal_csv,"market",time_to_be_noted)
        glob.trades += 1


def check_signal_file(signal_csv: pd.DataFrame, config: EasyDict):
    """Check the signal file for the backtesting

    Args:
        signal_csv (pd.DataFrame): Signal file generated for the backtesting
        config (EasyDict): Configuration for the backtesting
    """
    capital = config.backtester.capital
    pnl = 0
    transaction_cost = 0
    slippage = config.backtester.slippage
    status = 0
    entry_price = 0
    total_fee = 0
    leverage = config.backtester.leverage
    for i in range(len(signal_csv)):
        signal = signal_csv["signals"].iloc[i]
        exit_price = signal_csv["close"].iloc[i]
        if status == 0:
            if signal == 1:
                status = 1
                transaction_cost = capital * slippage
                entry_price = exit_price
            elif signal == -1:
                status = -1
                transaction_cost = capital * slippage
                entry_price = exit_price
        elif status == 1:
            if signal == -2:
                pnl = capital * (exit_price - entry_price) / entry_price * status * leverage
                capital += pnl
                capital -= transaction_cost
                total_fee += transaction_cost
                transaction_cost = capital * slippage
                entry_price = exit_price
                status = -1
            elif signal == -1:
                pnl = capital * (exit_price - entry_price) / entry_price * status * leverage
                capital += pnl
                capital -= transaction_cost
                total_fee += transaction_cost
                transaction_cost = 0
                status = 0
        elif status == -1:
            if signal == 2:
                pnl = capital * (exit_price - entry_price) / entry_price * status * leverage
                capital += pnl
                capital -= transaction_cost
                total_fee += transaction_cost
                transaction_cost = capital * slippage
                entry_price = exit_price
                status = 1
            elif signal == 1:
                pnl = capital * (exit_price - entry_price) / entry_price * status * leverage
                capital += pnl
                capital -= transaction_cost
                total_fee += transaction_cost
                transaction_cost = 0
                status = 0
