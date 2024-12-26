import pandas as pd
from utils import load_high_low, get_cfg, convert_to_open_timings
from easydict import EasyDict
from backtesting_ps_code import generate_signals, check_signal_file
from strategy import EMAStrategy, ButterChebyStrategy
from metrics import compute_metrics
from datetime import datetime
from pprint import pprint
from time import perf_counter



def time_taken(func):
    def wrapper(*args, **kwargs):
        start = perf_counter()
        func(*args, **kwargs)
        end = perf_counter()
        print(f"Time taken: {end-start}")
    return wrapper


@time_taken
def backtest(config: EasyDict):
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

    high_csv, low_csv, low_time = load_high_low(get_cfg())

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
        low_time,
        margin=get_cfg().backtester.margin,
        leverage=get_cfg().backtester.leverage,
        trailing=get_cfg().backtester.trailing,
        slippage=get_cfg().backtester.slippage,
        capital=get_cfg().backtester.capital,
        # entry_date=datetime(2018, 1, 1),
        # exit_date=datetime(2018, 2, 20),
    )

    signal_csv = signal_csv.drop(columns=["signal_type"])
    if get_cfg().backtester.print_metrics:
        metrics(signal_csv)


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
    # check_signal_file(signal_csv, get_cfg())

