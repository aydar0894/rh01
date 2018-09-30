import numpy as np
import pandas as pd
import random

from .PreProcessing import get_HistoricalData_FX

###############################################################################
#                  COMPUTE LOG RETURNS                                        #
###############################################################################
def calculate_LogReturns_FX( df_HistoricalMarketData_Exchange,
                             ccy_FOR                         ,
                             ccy_DOM                         ):

    df_historical = get_HistoricalData_FX( df_HistoricalMarketData_Exchange = df_HistoricalMarketData_Exchange,
                                           ccy_FOR                          = ccy_FOR,
                                           ccy_DOM                          = ccy_DOM)

    if df_historical.empty:
        return np.array([])


    df_historical_sorted = df_historical.copy() # make a deepcopy, needed for sorting

    df_historical_sorted['date'] = pd.to_datetime(df_historical_sorted['date'], format='%Y/%m/%d')
    df_historical_sorted  = df_historical_sorted.sort_values(by='date')

    df_historical_sorted['log_return'] = np.log(df_historical_sorted['rate']) - \
                                         np.log(df_historical_sorted['rate'].shift(1))
    # remove first row, because the log_return is NaN
    df_historical_sorted = df_historical_sorted.drop(df_historical_sorted.index[0])
    df_historical_sorted = df_historical_sorted.reset_index(drop=True)

    return df_historical_sorted['log_return'].values

###############################################################################
#                  READ RANDOM INDICES                                        #
###############################################################################
def read_RandomIndices( df_RandomIndices,
                        n_indices       ,
                        rowSelection    ,
                        n_logReturns    ):
    """
    get RandomIndices from DataFrame that was created based on csv file.
    There's two choices for the RandomIndex file:
        [1] Read Integers (Return Picks)
        [2] Read Uniform distribution in [0,1] that needs to be converted to [1]

    This function converts [2] to [1] if necessary, namely if index is 0 < idx < 1

    The index_array that is returned is in range [0 , n_logReturns-1]
    """

    if n_indices > df_RandomIndices.shape[0]:
        print('[ERROR]. Not enough RandomIndices provided to select n_indices')
        print('         nRows(df_RandomIndices) = ' + str(df_RandomIndices.shape[0]))
        print('         n_indices               = ' + str(n_indices))
        print('         Skipping FX_rate')

        return(np.array([]))

    first_RandomValue = df_RandomIndices[rowSelection].values[0]
    last_RandomValue  = df_RandomIndices[rowSelection].values[-1]

    #----------------#
    # float values   #
    #----------------#
    if first_RandomValue > 0.0 and first_RandomValue < 1.0 and \
       last_RandomValue  > 0.0 and last_RandomValue  < 1.0:
        idx_selected = df_RandomIndices[rowSelection].values
        idx_selected = (np.floor(idx_selected*n_logReturns)).astype(int)

    #----------------#
    # integer values #
    #----------------#
    else:
        idx_selected = df_RandomIndices[rowSelection].values-1

    # every value that is larger than max_value will be replaced, so that range = [0 , n_logReturns-1]
    idx_selected[idx_selected >= n_logReturns] = n_logReturns-1

    return idx_selected

###############################################################################
#                  COMPUTE RANDOM INDICES                                     #
###############################################################################
def select_RandomIndices( n_indices,
                          n_LogReturns):
    idx_selected = [ random.randint(0, n_LogReturns-1) for x in range(0, n_indices) ]
    return idx_selected

###############################################################################
#                  COMPUTE Ito TERM                                           #
###############################################################################
def calculate_ItoTerm( arr_logReturns, n):
    std          = np.std(arr_logReturns) # computes population std by default
    arr_ito_term = -0.5 * std**2 * np.arange(1, n+1)

    return arr_ito_term

###############################################################################
#                  COMPUTE Shift TERM                                         #
###############################################################################
def calculate_ShiftTerm( arr_logReturnsRescaled,
                         stressed_vol          ,
                         n                     ):

    mean_rescaled = np.mean(arr_logReturnsRescaled)
    arr_shift = (-0.5 * stressed_vol**2 * np.arange(1, n+1)) - \
                (mean_rescaled*np.arange(1, n+1))

    return arr_shift

###############################################################################
#                  COMPUTE 1 PRIIPS PATH (Fav, Mod, Unfav)                    #
###############################################################################
def calculate_PRIIPsPath_FMU( arr_randomIndices,
                              arr_logReturns   ,
                              arr_ItoTerm      ,
                              spot_rate        ):



    arr_returns        = arr_logReturns[arr_randomIndices]
    arr_returns_cumsum = np.cumsum(arr_returns)
    arr_underlying     = spot_rate*np.exp(arr_returns_cumsum + arr_ItoTerm)

    return arr_underlying


###############################################################################
#                  COMPUTE 1 PRIIPS PATH (Stressed)                           #
###############################################################################
def calculate_PRIIPsPath_Stressed( arr_randomIndices     ,
                                   arr_logReturnsRescaled,
                                   arr_shiftTerm         ,
                                   spot_rate             ):

        arr_returns        = arr_logReturnsRescaled[arr_randomIndices]
        arr_returns_cumsum = np.cumsum(arr_returns)
        arr_underlying     = spot_rate*np.exp(arr_returns_cumsum + arr_shiftTerm)

        return arr_underlying

###############################################################################
#                  COMPUTE THE STRESSED VOLATILITY                            #
###############################################################################
def calculate_stressed_vol(arr_returns, window_length):
    arr_rolling_returns = (rolling_window(arr_returns, window_length+1))

    arr_rolling_vol = np.array([])

    for arr_ret_window in arr_rolling_returns:
        arr_rolling_vol = np.append(arr_rolling_vol, np.std(arr_ret_window))

    stressed_vol = None

    # the percentile used for rolling vol depends on window length
    if (window_length == 12) or \
       (window_length == 16) or \
       (window_length == 63):
        stressed_vol = np.percentile(arr_rolling_vol, 90)

    if (window_length == 6) or \
       (window_length == 8) or \
       (window_length == 21):
        stressed_vol = np.percentile(arr_rolling_vol, 99)

    return stressed_vol


###############################################################################
#                  COMPUTE A ROLLING WINDOW OF ARRAY                          #
###############################################################################
def rolling_window(arr, window):
    idx_windowStart = 0
    idx_windowEnd   = window

    arr_window = np.array([])
    list_window = []

    while True:
        if len(arr) < (idx_windowStart+idx_windowEnd):
            break
        else:
            new_window = arr[idx_windowStart : (idx_windowStart+idx_windowEnd)]
            list_window.append(new_window)

            idx_windowStart += 1

    return np.array(list_window)
