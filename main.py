import pandas as pd
from utils import load_high_low, get_cfg, convert_to_open_timings, to_minutes, handle_date_time
from easydict import EasyDict
from backtesting_ps_code import generate_signals, check_signal_file
from strategy import EMAStrategy, ButterChebyStrategy
from metrics import compute_metrics
from datetime import datetime
from pprint import pprint



def main():
    trade_sheet = pd.DataFrame(
        columns=[
            "date_time",
            "executed_price",
            "capital",
            "signal",
            "order_status",
            "order_type",
            "profit_loss%",
            "stop_loss",
        ]
    )
    signal_csv = pd.DataFrame(
        columns=["datetime", "open", "high", "low", "close", "volume", "signals","signal_type"],
    )

    high_csv, low_csv = load_high_low(get_cfg())

    entry_time = handle_date_time(get_cfg().data.start_date)
    exit_time = handle_date_time(get_cfg().data.end_date)

    # Write to make low_csv "datetime" lie between entry_time and exit_time using handle_date_time
    low_csv = low_csv[(low_csv["datetime"].apply(handle_date_time) >= entry_time) & (low_csv["datetime"].apply(handle_date_time) <= exit_time)]
    low_csv = low_csv.reset_index(drop=True)


    GLOB = EasyDict(
        tp=get_cfg().backtester.tp,  # Target Price Percentage
        sl=get_cfg().backtester.sl,  # Stop Loss Percentage
        entry_price=1,
        trailing_price=0,  # Trailing Price of the current position of the trade (used for the calculation of trailing stop loss)
        date_time=high_csv.loc[
            0, "datetime"
        ],  # Intialzing the start date of the backtesting
        status=0,  # Initialing the status as 0 (no position currently)
        total_fee=0,
        trades=0,
    )

    strat = ButterChebyStrategy(high_csv, low_csv, get_cfg(), GLOB)

    generate_signals(
        strat,
        GLOB,
        high_csv,
        low_csv,
        trade_sheet,
        signal_csv,
        low_time = to_minutes(get_cfg().backtester.low_time),
        high_time = to_minutes(get_cfg().backtester.high_time),
        margin=get_cfg().backtester.margin,
        leverage=get_cfg().backtester.leverage,
        trailing=get_cfg().backtester.trailing,
        slippage=get_cfg().backtester.slippage,
        capital=get_cfg().backtester.capital,
        entry_date=entry_time,
        exit_date=exit_time,
    )

    trade_sheet.to_csv("trade_sheet.csv", index=False)
    signal_csv = signal_csv.drop(columns=["signal_type"])
    signal_csv.to_csv("signal_csv.csv", index=False)
    return signal_csv


def metrics(signal_csv: pd.DataFrame):
    pprint(
        compute_metrics(
            signal_csv,
            get_cfg().backtester.plots.show,
            get_cfg().backtester.leverage,
            get_cfg().backtester.slippage,
            get_cfg().backtester.capital,
        )
    )
    check_signal_file(signal_csv, get_cfg())


if __name__ == "__main__":
    signal_csv = main()
    
    # signal_csv = pd.read_csv("signal_csv.csv")
    if get_cfg().backtester.print_metrics:
        metrics(signal_csv)
