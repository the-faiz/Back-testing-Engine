import yaml
from easydict import EasyDict
import pandas as pd
from pathlib import Path
from typing import Tuple
from datetime import datetime,timedelta
from time import perf_counter

CACHE = {}


def get_cfg(file_path: str = "config.yaml") -> EasyDict:
    """Get the config yaml file as an EasyDict object.

    Parameters:
        file_path (str): The path to the config yaml file.
    Returns:
        EasyDict: The yaml file as an EasyDict object.
    """
    if file_path not in CACHE:
        CACHE[file_path] = _load_cfg(file_path)
    if CACHE[file_path] is None:
        raise FileNotFoundError(f"Config file not found at {file_path}")
    return CACHE[file_path]


def _load_cfg(file_path: str = "config.yaml") -> EasyDict | None:
    """Load the config yaml file as an EasyDict object.

    Parameters:
        file_path (str): The path to the config yaml file.
    Returns:
        EasyDict: The yaml file as an EasyDict object.
    """
    with open(file_path, "r") as stream:
        try:
            return EasyDict(yaml.safe_load(stream))
        except yaml.YAMLError as exc:
            print(exc)
            return None


def load_high_low(
    config: EasyDict
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load the high and low timeframe dataframes

    Args:
        config (EasyDict): Config object containing the data file paths and strategy parameters

    Returns:
        Tuple (pd.DataFrame, pd.DataFrame): DataFrames containing the high and low timeframe data
    """
    high_time = config.backtester.high_time
    low_time = to_minutes(config.backtester.low_time)
    DATA_DIR = Path(config.data.path)

    load_time = perf_counter()
    high_csv = pd.read_csv(DATA_DIR / config.data.files[config.backtester.high_time])
    # print(f"High CSV loaded in {perf_counter() - load_time:.4f} seconds")
    load_time = perf_counter()
    low_csv = pd.read_csv(DATA_DIR / config.data.files[config.backtester.low_time])
    # print(f"Low CSV loaded in {perf_counter() - load_time:.4f} seconds")
    return high_csv, low_csv

def handle_date_time(date_time: str) -> datetime:
    try:
        # Try parsing with microseconds
        date_object = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        # Fallback to parsing without microseconds if ValueError is raised
        try:
            date_object = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            date_object = datetime.strptime(date_time, "%Y-%m-%d")
            
    return date_object


def day_low_csv(low_pointer: int, low_csv: pd.DataFrame) -> datetime:
    """Get the day of the low timeframe data

    Args:
        low_pointer (int): Pointer to the low timeframe data
        low_csv (pd.DataFrame): DataFrame containing the low timeframe data

    Returns:
        int: Day of the low timeframe data
    """
    return handle_date_time(low_csv["datetime"].iloc[low_pointer])


def day_high_csv(high_pointer: int, high_csv: pd.DataFrame) -> datetime:
    """Get the day of the high timeframe data

    Args:
        high_pointer (int): Pointer to the high timeframe data
        high_csv (pd.DataFrame): DataFrame containing the high timeframe data

    Returns:
        int: Day of the high timeframe data
    """
    # date_object = pd.to_datetime(high_csv["datetime"].iloc[high_pointer])
    # # print(date_object)

    return handle_date_time(high_csv["datetime"].iloc[high_pointer])


def trade_log(
    date_time: str,
    executed_price: float,
    capital: float,
    signal: int,
    status: int,
    order_type: str,
    p: float,
    stop_loss: float,
    trade_sheet: pd.DataFrame,
):
    """Log the trade details

    Args:
        date_time (str): Timestamp of the trade
        executed_price (float): Price at which the trade was executed
        capital (float): Capital at the time of trade
        signal (int): Signal for the trade
        status (int): Status of the trade
        order_type (str): Type of the order
        p (float): Profit/Loss percentage
        stop_loss (float): Stop loss price
        trade_sheet (pd.DataFrame): DataFrame to log the trade details
    """
    if status == 1:
        order_status = "LONG"
    elif status == -1:
        order_status = "SHORT"
    else:
        order_status = "Squared_Off"

    log = {
        "date_time": date_time,
        "executed_price": executed_price,
        "capital": capital,
        "signal": signal,
        "order_status": order_status,
        "order_type": order_type,
        "profit_loss%": p,
        "stop_loss": stop_loss,
    }
    trade_sheet.loc[len(trade_sheet)] = log


def adjust(low_pointer: int, high_pointer: int, low_csv: int, high_csv: int) -> int:
    """Adjust the low pointer to match the high pointer

    Args:
        low_pointer (int): pointer to the low timeframe data
        high_pointer (int): pointer to the high timeframe data
        low_csv (int): low timeframe data
        high_csv (int): high timeframe data

    Returns:
        int: Adjusted low pointer
    """
    while handle_date_time(high_csv['datetime'].iloc[high_pointer])>handle_date_time(low_csv['datetime'].iloc[low_pointer]):
        low_pointer += 1
    return low_pointer


def generate_csv(
    ptr: int,
    csv_value: int,
    signal: int,
    low_csv: pd.DataFrame,
    high_csv: pd.DataFrame,
    signal_csv: pd.DataFrame,
    signal_type: str,
    time_to_open: datetime
):
    """Generate the Signal CSV

    Args:
        ptr (int): Pointer to the data, either high or low
        csv_value (int): 0 for low and 1 for high timeframe data
        signal (int): Signal for the trade
        low_csv (pd.DataFrame): low timeframe data
        high_csv (pd.DataFrame): high timeframe data
        signal_csv (pd.DataFrame): DataFrame to log the signal details
    """
    if csv_value == 0:
        data = low_csv.iloc[ptr]
    else:
        data = high_csv.iloc[ptr]
    log = {
        # "datetime": data["datetime"],
        "datetime": time_to_open,
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"],
        "signals": signal,
        "signal_type": signal_type,
    }
    signal_csv.loc[len(signal_csv)] = log


def tpsl(
    low_pointer: int,
    high_pointer: int,
    low_csv: pd.DataFrame,
    high_csv: pd.DataFrame,
    margin: float,
    leverage: int,
    glob: EasyDict,
    trailing=False,
) -> Tuple[int, int]:
    """Function to check the target price and stop loss conditions

    Args:
        low_pointer (int): Pointer to the low timeframe data
        low_csv (pd.DataFrame): DataFrame containing the low timeframe data
        margin (float): Margin for the trade
        leverage (int): Leverage for the trade
        glob (EasyDict): Global variables containing the trade details
        trailing (bool, optional): Is the Stop loss trailing. Defaults to False.

    Returns:
        Tuple: (int, int)
        - 1 if the condition is satisfied, 0 otherwise
        - Index at which the condition is satisfied
    """
    if high_pointer+1==len(high_csv):
        tpsl_check_end_time = handle_date_time("3030-01-01")
    else:
        tpsl_check_end_time = handle_date_time(high_csv['datetime'].iloc[high_pointer+1])
    margin_price = glob.entry_price - glob.status * margin / leverage * glob.entry_price
    index = int(low_pointer)
    # print("!Starting TPSL check from ", low_csv["datetime"].iloc[index])
    while index<len(low_csv) and tpsl_check_end_time>handle_date_time(low_csv['datetime'].iloc[index]):
        Close = low_csv["close"].iloc[index]
        if glob.status == 1:
            glob.trailing_price = max(glob.trailing_price, Close)
        if glob.status == -1:
            glob.trailing_price = min(glob.trailing_price, Close)
        target_price = glob.entry_price + glob.status * glob.entry_price * glob.tp
        stop_loss = glob.entry_price - glob.status * glob.entry_price * glob.sl
        trailing_stop_loss = (
            glob.trailing_price - glob.status * glob.trailing_price * glob.sl
        )
        if glob.status == 1 and (
            target_price <= Close
            or stop_loss >= Close
            or (trailing and trailing_stop_loss >= Close)
            or margin_price >= Close
        ):
            date_time = low_csv["datetime"].iloc[index]
            # print("TPSL hit at ", date_time)
            return 1, index
        if glob.status == -1 and (
            target_price >= Close
            or stop_loss <= Close
            or (trailing and trailing_stop_loss <= Close)
            or margin_price <= Close
        ):
            date_time = low_csv["datetime"].iloc[index]
            # print("TPSL hit at ", date_time)
            return 1, index
        index += 1
    # print("Ending TPSL check at ", low_csv["datetime"].iloc[index])
    return 0, 0

def to_minutes(time: str) -> int:
    """Convert the time to minutes

    Args:
        time (str): Time in 1d, 1h, 1m format

    Returns:
        int: Time in minutes
    """
    unit = time[-1]
    value = int(time[:-1])
    match unit:
        case "d": return value * 24 * 60
        case "h": return value * 60
        case "m": return value
        case "w": return value * 7 * 24 * 60
        case _: raise ValueError(f'Invalid unit: {unit}')
        

def check_if_exists_in_next_15mins(
    temp_pointer: int, low_csv: pd.DataFrame, current_date: datetime, future_time_diff: int = 15
) -> Tuple[bool, datetime]:
    """Check if the signal exists in the next 15 minutes

    Args:
        temp_pointer (int): Pointer to the low timeframe data
        low_csv (pd.DataFrame): DataFrame containing the low timeframe data
        current_date (datetime): Current date
        future_time_diff (int, optional): Time difference to check. Defaults to 15.

    Returns:
        Tuple: (bool, datetime)
        - True if the signal exists in the next 15 minutes, False otherwise
        - Date at which the signal exists
    """
    if temp_pointer < len(low_csv):
        if (handle_date_time(low_csv.loc[temp_pointer, "datetime"]) - current_date).seconds / 60 <= future_time_diff:
            return True, low_csv.loc[temp_pointer, "datetime"]
    return False, None

def convert_to_open_timings(
        current_time_m: datetime,
        low_csv: pd.DataFrame,
        open_time_lower_pointer: int,
        low_time: int,
        high_time: int,
        future_time_diff: int = 15
):
            
            current_date = current_time_m
            lower_pointer = open_time_lower_pointer
            current_date = current_date + timedelta(minutes=high_time)
            print(type(current_date))
            while lower_pointer<len(low_csv) and handle_date_time(low_csv.loc[lower_pointer , 'datetime']) < current_date:
                lower_pointer += 1
            prev_pointer = lower_pointer-1
            if prev_pointer>=0 and (current_date - handle_date_time(low_csv.loc[prev_pointer , 'datetime'])).seconds/60 <=low_time:
                return True, low_csv.loc[prev_pointer , 'datetime'], prev_pointer + 1
            else:
                temp_pointer = lower_pointer
                stats , val = check_if_exists_in_next_15mins(temp_pointer , low_csv , current_date , future_time_diff)
                if stats:
                    return True, val, temp_pointer + 1
                else:
                    return False, None, lower_pointer


