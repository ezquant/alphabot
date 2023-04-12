import OSConfig as Config
import platform
import enum
import pandas as pd
import re


def get_os_system() -> Config.OsSystem:
    sys = platform.system()
    # print(f"system is {sys}")
    if sys == "Linux":
        return Config.OsSystem.LINUX
    elif sys == "Darwin":
        return Config.OsSystem.MAC
    elif sys == "Windows":
        return Config.OsSystem.WINDOWS
    return Config.OsSystem.UNKNOWN


def is_df_none_or_empty(df: pd.DataFrame) -> bool:
    return df is None or df.empty


sh_regex = re.compile(r'0.*')
sz_regex = re.compile(r'6.*')

# return 0 as Shanghai, 1 as Shenzheng, -1 unknown
# NOT accurate, need to be refactored.
def get_stock_market(stock_id: str) -> int:
    if re.match(sh_regex, stock_id):
        return 0
    elif re.match(sz_regex, stock_id):
        return 1
    else:
        return -1


def sort_enum(enum_list: [], reverse: bool = False):
    return sorted(enum_list, key=__sort, reverse=reverse)


def __sort(en: enum.Enum):
    return en.value
