import numpy as np
import pandas as pd
from scipy import interpolate

###############################################################################
#        INTERPOLATE IN ONE DIMENSION                                         #
###############################################################################
def interpolate_1D(arr_x, arr_y, x_interpolate, kind='linear'):
    """
    interpolate between arr_x and arr_y, 
    and calculate y value associated with x_interpolate.
    For YieldCurve interpolation: arr_x: tenors, arr_y: yields
    
    kind can be linear, cubic, quadratic, nearest, previous, next
    """
    obj_interpolate = interpolate.interp1d(arr_x, arr_y, kind=kind)
    return obj_interpolate(x_interpolate)
    
###############################################################################
#        CALCULATE STRIKE                                                     #
###############################################################################
def calculate_strike(yield_FOR                 , \
                     yield_DOM                 , \
                     spot_rate                 , \
                     T                         , \
                     n_compoundingPeriods      ):

        strike = spot_rate * \
                 np.power((1.0 + yield_DOM/n_compoundingPeriods), n_compoundingPeriods*T) / \
                 np.power((1.0 + yield_FOR/n_compoundingPeriods), n_compoundingPeriods*T)

        return strike

###############################################################################
#        CALCULATE FORWARD RATE                                               #
###############################################################################
def calculate_ForwardRate( df_CurrentMarketData_Yield , \
                           dict_YieldCcy_CurveName    , \
                           tenor_in_days_1            , \
                           tenor_in_days_2            , \
                           ccy_pair                   , \
                           ccy_name                   , \
                           T_1                        , \
                           T_2                        , \
                           n_compoundingPeriods       ):
                           
    yield_1 = get_Yield( df_CurrentMarketData_Yield = df_CurrentMarketData_Yield, \
                         dict_YieldCcy_CurveName    = dict_YieldCcy_CurveName   , \
                         tenor_in_days              = tenor_in_days_1           , \
                         ccy_pair                   = ccy_pair                  , \
                         ccy_name                   = ccy_name)
                         
    yield_2 = get_Yield( df_CurrentMarketData_Yield = df_CurrentMarketData_Yield, \
                         dict_YieldCcy_CurveName    = dict_YieldCcy_CurveName   , \
                         tenor_in_days              = tenor_in_days_2           , \
                         ccy_pair                   = ccy_pair                  , \
                         ccy_name                   = ccy_name)
    
    ForwardRate = np.power((1.0 + yield_2/n_compoundingPeriods), T_2*n_compoundingPeriods) / \
                  np.power((1.0 + yield_1/n_compoundingPeriods), T_1*n_compoundingPeriods)
                  
    return ForwardRate
                                             
###############################################################################
#        EXTRACT SPOT PRICE                                                   #
###############################################################################
def get_SpotRate(df_CurrentMarketData_Exchange, \
                 ccy_FOR                      , \
                 ccy_DOM):
    df_spot_rate = df_CurrentMarketData_Exchange[ (df_CurrentMarketData_Exchange['ccy_FOR'] == \
                                                  ccy_FOR) &                                   \
                                                  (df_CurrentMarketData_Exchange['ccy_DOM'] == \
                                                  ccy_DOM) ]
    if df_spot_rate.empty:
        print('[ERROR]. FX Spot rate not found...')
        print('         FOR: ' + product.ccy_FOR)
        print('         DOM: ' + product.ccy_DOM)
        print('         Skipping to next product')
        return None
    else:
        spot_rate = df_spot_rate['rate'].values[0] 
        return spot_rate


###############################################################################
#        EXTRACT YIELD                                                        #
###############################################################################
def get_Yield( df_CurrentMarketData_Yield , \
               dict_YieldCcy_CurveName    , \
               tenor_in_days              , \
               ccy_pair                   , \
               ccy_name                   ):
    """
    interpolation between tenor points is done linearly
    """
    # pick currency pair
    df_yieldcurve = df_CurrentMarketData_Yield[ df_CurrentMarketData_Yield['curvename']  == \
                                                dict_YieldCcy_CurveName[ccy_pair][ccy_name] ]
    
    yield_interpolated = interpolate_1D(arr_x=df_yieldcurve['tenor'].values, arr_y=df_yieldcurve['yield'].values, x_interpolate=tenor_in_days, kind='linear')
    return np.asscalar(yield_interpolated)

###############################################################################
#        EXTRACT HISTORICAL MARKETDATA                                        #
############################################################################### 
def get_HistoricalData_FX( df_HistoricalMarketData_Exchange , \
                           ccy_FOR                          , \
                           ccy_DOM ):
    
    df_historical = pd.DataFrame() 
    df_historical = df_HistoricalMarketData_Exchange[ (df_HistoricalMarketData_Exchange['ccy_FOR'] == ccy_FOR) & \
                                                      (df_HistoricalMarketData_Exchange['ccy_DOM'] == ccy_DOM) ]
    if df_historical.empty:
        print('[ERROR]. Could not read Historical Market Data (FX) for:' + ccy_FOR+ccy_DOM)
        return pd.DataFrame()
    else:
        return df_historical
                                                      
###############################################################################
#        EXTRACT ATM Volatility                                               #
###############################################################################
def get_FXATMVol( df_CurrentMarketData_FXATMVol , \
                  dict_FXATMVol_CurveName       , \
                  tenor_in_days                 , \
                  ccy_name                      , \
                  ccy_name_counter              ):
    """
    interpolate between tenors using cubic splines
    """    
    # pick currency pair
    df_FXATMcurve = df_CurrentMarketData_FXATMVol[ (df_CurrentMarketData_FXATMVol['curvename']   == \
                                                    dict_FXATMVol_CurveName[ccy_name]) &            \
                                                   (df_CurrentMarketData_FXATMVol['ccy_counter'] == \
                                                    ccy_name_counter)                               ]

    
    
    vol_interpolated = interpolate_1D( arr_x         = df_FXATMcurve['tenor'].values     , \
                                       arr_y         = df_FXATMcurve['volatility'].values, \
                                       x_interpolate = tenor_in_days                     , \
                                       kind          = 'cubic')
    return np.asscalar(vol_interpolated)

###############################################################################
#        EXTRACT MarketStrangle Volatility                                    #
###############################################################################
def get_FX_MS_Vol( df_CurrentMarketData_FXDeltaVol , \
                   dict_FXDeltaVol_CurveName        , \
                   tenor_in_days                    , \
                   ccy_name                         , \
                   ccy_name_counter                 , \
                   DeltaValue                       ):
    """
    interpolate between tenors using cubic splines
    """
    
    # pick currency pair and Delta value
    df_FXDeltacurve = df_CurrentMarketData_FXDeltaVol[ (df_CurrentMarketData_FXDeltaVol['curvename']   == \
                                                        dict_FXDeltaVol_CurveName[ccy_name])            & \
                                                       (df_CurrentMarketData_FXDeltaVol['ccy_counter'] == \
                                                        ccy_name_counter)                               & \
                                                       (df_CurrentMarketData_FXDeltaVol['optionType']  == \
                                                        'MS')                                           & \
                                                       (df_CurrentMarketData_FXDeltaVol['DeltaValue']  == \
                                                        DeltaValue)                                       ]

    vol_interpolated = interpolate_1D( arr_x         = df_FXDeltacurve['tenor'].values     , \
                                       arr_y         = df_FXDeltacurve['volatility'].values, \
                                       x_interpolate = tenor_in_days                       , \
                                       kind          = 'cubic')
    DeltaFlag = df_FXDeltacurve['DeltaFlag'].values[0]
    return np.asscalar(vol_interpolated), DeltaFlag
    
###############################################################################
#        EXTRACT RiskReversal Volatility                                      #
###############################################################################
def get_FX_RR_Vol( df_CurrentMarketData_FXDeltaVol , \
                   dict_FXDeltaVol_CurveName        , \
                   tenor_in_days                    , \
                   ccy_name                         , \
                   ccy_name_counter                 , \
                   DeltaValue                       ):
    """
    interpolate between tenors using cubic splines
    """
    
    # pick currency pair and Delta value
    df_FXDeltacurve = df_CurrentMarketData_FXDeltaVol[ (df_CurrentMarketData_FXDeltaVol['curvename']   == \
                                                        dict_FXDeltaVol_CurveName[ccy_name])            & \
                                                       (df_CurrentMarketData_FXDeltaVol['ccy_counter'] == \
                                                        ccy_name_counter)                               & \
                                                       (df_CurrentMarketData_FXDeltaVol['optionType']  == \
                                                        'RR')                                           & \
                                                       (df_CurrentMarketData_FXDeltaVol['DeltaValue']  == \
                                                        DeltaValue)  
                                                        ]
    
    vol_interpolated = interpolate_1D( arr_x         = df_FXDeltacurve['tenor'].values     , \
                                       arr_y         = df_FXDeltacurve['volatility'].values, \
                                       x_interpolate = tenor_in_days                     , \
                                       kind          = 'cubic')
    DeltaFlag = df_FXDeltacurve['DeltaFlag'].values[0]
    return np.asscalar(vol_interpolated), DeltaFlag

###############################################################################
#        CONVERT TRADE-DATE TO str_DateIdentifier                             #
###############################################################################    
def getStrDateIdentifier(str_tradeDate):
    """
    str_dateIdentifier is a unique string representing the tradeDate.
    It is used to read in MarketData files coming out from MDM, and also
    used to generate date_Identifiers for output files (paths, indices, ...)
    """
    
    list_str = str_tradeDate.split("/")
    
    # sanity checks of format
    if len(list_str) != 3    or \
       int(list_str[0]) < 1  or \
       int(list_str[0]) > 31 or \
       int(list_str[1]) < 1  or \
       int(list_str[1]) > 12 or \
       len(list_str[0]) != 2 or \
       len(list_str[1]) != 2 or \
       len(list_str[2]) != 4 :
        print('[ERROR]. The tradeDate format is wrong. It needs to be DD/MM/YYYY')
        print('         ---------- Aborting ---------')
        return None
        
    else:
        return list_str[2] + list_str[1] + list_str[0]