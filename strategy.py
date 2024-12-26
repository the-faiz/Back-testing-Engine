from scipy.signal import butter, cheby1, filtfilt
import pandas as pd
import numpy as np
from easydict import EasyDict


class BaseStrategy:
    def __init__(
        self,
        high_csv: pd.DataFrame,
        low_csv: pd.DataFrame,
        config: EasyDict,
        glob: EasyDict,
    ):
        self.high_csv = high_csv
        self.low_csv = low_csv
        self.config = config
        self.glob = glob
        self.preprocessing()

    def preprocessing(self):
        pass

    def check_long_entry(self, high_pointer: int):
        pass

    def check_short_entry(self, high_pointer: int):
        pass

    def check_long_exit(self, high_pointer: int):
        pass

    def check_short_exit(self, high_pointer: int):
        pass


class EMAStrategy(BaseStrategy):
    def preprocessing(self):
        self.high_csv["long_EMA"] = (
            self.high_csv["close"].ewm(span=12, adjust=False).mean()
        )
        self.high_csv["short_EMA"] = (
            self.high_csv["close"].ewm(span=9, adjust=False).mean()
        )

    def check_long_entry(self, high_pointer: int):
        long_ema = self.high_csv["long_EMA"].iloc[high_pointer]
        short_ema = self.high_csv["short_EMA"].iloc[high_pointer]
        Close = self.high_csv["close"].iloc[high_pointer]
        if short_ema > long_ema:
            self.glob.tp = 0.1
            self.glob.sl = 0.05
            self.glob.entry_price = Close
            self.glob.trailing_price = Close
            return 1
        return 0

    def check_short_entry(self, high_pointer: int):
        long_ema = self.high_csv["long_EMA"].iloc[high_pointer]
        short_ema = self.high_csv["short_EMA"].iloc[high_pointer]
        Close = self.high_csv["close"].iloc[high_pointer]
        if short_ema < long_ema:
            self.glob.tp = 0.1
            self.glob.sl = 0.05
            self.glob.entry_price = Close
            self.glob.trailing_price = Close
            return 1
        return 0

    def check_long_exit(self, high_pointer: int):
        return 0

    def check_short_exit(self, high_pointer: int):
        return 0


class ButterChebyStrategy(BaseStrategy):
    def butterworth(self, data: pd.DataFrame):
        order = self.config.strategies.strat_cheby.butterworth.order
        cutoff_freq = self.config.strategies.strat_cheby.butterworth.cutoff_frequency
        b, a = butter(N=order, Wn=cutoff_freq, btype="low", analog=False, output="ba")
        smooth_data = filtfilt(b, a, data["close"], padlen=0)
        return smooth_data

    def chebyshev(self, data: pd.DataFrame):
        cutoff_freq = self.config.strategies.strat_cheby.chebyshev.cutoff_frequency
        rp = self.config.strategies.strat_cheby.chebyshev.ripple_factor
        order = self.config.strategies.strat_cheby.chebyshev.order
        b, a = cheby1(
            N=order, rp=rp, Wn=cutoff_freq, btype="low", analog=False, output="ba"
        )
        smooth_data = filtfilt(b, a, data["close"], padlen=0)
        return smooth_data

    def preprocessing(self):
        self.high_csv["butter"] = 0.0
        self.high_csv["cheby"] = 0.0
        df_temp = pd.DataFrame(columns=self.high_csv.columns)
        for i in range(1, len(self.high_csv) + 1):
            df_temp.loc[len(df_temp)] = self.high_csv.loc[i - 1]
            self.high_csv.loc[i - 1, "butter"] = self.butterworth(df_temp)[-1]
            self.high_csv.loc[i - 1, "cheby"] = self.chebyshev(df_temp)[-1]

    def check_long_entry(self, high_pointer: int):
        i = high_pointer
        if not i:
            return False
        c1 = self.high_csv.loc[i, "cheby"] > self.high_csv.loc[i, "butter"]
        c2 = self.high_csv.loc[i - 1, "cheby"] < self.high_csv.loc[i - 1, "butter"]
        Close = self.high_csv.loc[i, "close"]
        if c1 and c2:
            self.glob.entry_price = Close
            self.glob.trailing_price = Close
            return True
        return False

    def check_short_entry(self, high_pointer: int):
        i = high_pointer
        if not i:
            return False
        c1 = self.high_csv.loc[i, "cheby"] < self.high_csv.loc[i, "butter"]
        c2 = self.high_csv.loc[i - 1, "cheby"] > self.high_csv.loc[i - 1, "butter"]
        Close = self.high_csv.loc[i, "close"]
        if c1 and c2:
            self.glob.entry_price = Close
            self.glob.trailing_price = Close
            return True
        return False

    def check_long_exit(self, high_pointer: int):
        return 0

    def check_short_exit(self, high_pointer: int):
        return 0
