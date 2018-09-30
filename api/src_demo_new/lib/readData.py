import pandas as pd
import numpy as np
import json
import datetime
import pyodbc
import os

###############################################################################
#        READ RANDOM INDICES                                                  #
###############################################################################
def readRandomIndexFile(filename):
    script_dir = os.path.dirname(__file__)
    """
    Read the RandomIndices from a file.

    There's two choices for the RandomIndex file:
        [1] Read Integers (Return Picks)
        [2] Read Uniform distribution in [0,1] that needs to be converted to [1]
    This function reads either of them, and returns them as DataFrame.
    There's a function in Bootstrap.py that converts indices if necessary.

    Return value: DataFrame with indices
    """
    df_RandomIndices = pd.read_csv( os.path.join(script_dir, filename)       , \
                                    sep      = ' ' , \
                                    header   = None  )

    return df_RandomIndices

###############################################################################
#        READ CURRENT MARKETDATA                                              #
###############################################################################
def readCurrentMarketData( str_dateIdentifier,
                           filenameSuffix = "-BARC-CurrentMarketData",
                           fileFormat     = "csv"):
    """
    Read in CurrentMarketData.csv as it comes from MDM process.
      [1] Split it into four sections: Exchange, Yield, FXDeltaVol, FXATMVol
      [2] Read each of the four files into a DataFrame

    Return value: five values:
      1       ... completedNoError [True/False]
      2,3,4,5 ... DataFrames containing MarketData
    """


    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.0.75.1,1433;DATABASE=rh01;UID=sa;PWD=qbasik007;TDS_Version=8.0;'

    cnxn = pyodbc.connect(
                         # r'DRIVER={SQL Server};'
                         # r'SERVER=DESKTOP-8CBSCDJ;'
                         # r'DATABASE=rh01;'
                         # r'Trusted_Connection=yes;'
                         connection_string
                         )
    #create cursor
    cursor = cnxn.cursor()

    #EQUITY Query
    sql_Equity        = """select ["type"],
                                 ["isin"],
                                 ["closepricedate"],
                                 ["pricecurrencyid"],
                                 ["closeprice"]
                        from [dbo].[mdm.price]
                        where type='Equity'"""
    #EXCHANGE Query
    sql_Exchange      = """select 'Exchange' as 'Exchange',
                                 ["curveID"],
                                 ["observationDate"],
                                 ["currencyID"],
                                 ["exchangeRate"]
                         from [dbo].[mdm.rate]
                         where ["observationDate"] like '"2018-05-31%"' """
    #FXAtmVol Query
    sql_FXATMVol      = """select ["curvetype"],
                                 ["curveID"] + '-' + 'ATM' as 'curveID',
                                 ["observationDate"],
                                 ["currencyID"],
                                 'lin' as 'lin',
                                 'near' as 'near',
                                 ["atmType"],
                                 ["deltaPremium"],
                                 ["daysPerAnnum"],
                                 ["holidaySet"],
                                 ["expiryUnits"],
                                 ["busDayConv"],
                                 ["invalidDateRule"],
                                 ["endOfMonthRule"],
                                 ["expiry"],
                                 ["vol"]
                        from [dbo].[mdm.surfaceATM]
                        where ["observationDate"] like '"2018-05-31%"' """
    #FXNonAtmVol Query
    sql_FXDeltaVolMtx = """select ["curvetype"],
                                     ["curveID"] + '-' + ["quoteInstrumentType"] as 'curveID',
                                     ["observationDate"],
                                     ["currencyID"],
                                      'lin' as 'lin',
                                      'near' as 'near',
                                      'lin' as 'lin',
                                      'near' as 'near',
                                     ["quoteInstrumentType"],
                                     ["deltaType"],
                                     ["deltaPremium"],
                                     ["daysPerAnnum"],
                                     ["holidaySet"],
                                     ["expiryUnits"],
                                     ["busDayConv"],
                                     ["invalidDateRule"],
                                     ["endOfMonthRule"],
                                     ["delta"],
                                     ["expiry"],
                                     ["vol"]
                             from [dbo].[mdm.surfaceOTM]
                             where ["observationDate"] like '"2018-05-31%"' """
    #Yield Curves Query
    sql_Yield        = """select 'Yield' as 'Yield',
                                  [dbo].[mdm.curve].["curveID"],
                                  [dbo].[mdm.curve].["observationDate"],
                                  [dbo].[mdm.curve].["currencyID"],
                                  'lin' as 'lin',
                                  'near' as 'near',
                                  'ACT365' as 'ACT365',
                                  'DAILY' as 'DAILY',
                                  [dbo].[mdm.curvepoint].["maturity"],
                                  [dbo].[mdm.curvepoint].["yield"]
                                    from
                                    [dbo].[mdm.curve] inner join [dbo].[mdm.curvepoint]
                                    on [dbo].[mdm.curve].["ID"]=[dbo].[mdm.curvepoint].["ID"]
                                    where ["observationDate"] like '"2018-05-31%"' """



    df_CurrentMarketData_Exchange     = pd.DataFrame( columns = [ 'type'    ,
                                                                  'ccy_FOR' ,
                                                                  'date'    ,
                                                                  'ccy_DOM' ,
                                                                  'rate'    ] )

    df_CurrentMarketData_Yield        = pd.DataFrame( columns = [ 'type'      ,
                                                                  'curvename' ,
                                                                  'date'      ,
                                                                  'ccy_yield' ,
                                                                  'ignore1'   ,
                                                                  'ignore2'   ,
                                                                  'ignore3'   ,
                                                                  'ignore4'   ,
                                                                  'tenor'     ,
                                                                  'yield'     ] )

    df_CurrentMarketData_FXATMVol     = pd.DataFrame( columns = [ 'type'        ,
                                                                  'curvename'   ,
                                                                  'date'        ,
                                                                  'ccy_counter' ,
                                                                  'ignore1'     ,
                                                                  'ignore2'     ,
                                                                  'ignore3'     ,
                                                                  'ignore4'     ,
                                                                  'ignore5'     ,
                                                                  'ignore6'     ,
                                                                  'ignore7'     ,
                                                                  'ignore8'     ,
                                                                  'ignore9'     ,
                                                                  'ignore10'    ,
                                                                  'tenor'       ,
                                                                  'volatility'  ] )

    df_CurrentMarketData_FXDeltaVol     = pd.DataFrame( columns = [ 'type'        ,
                                                                    'curvename'   ,
                                                                    'date'        ,
                                                                    'ccy_counter' ,
                                                                    'ignore1'     ,
                                                                    'ignore2'     ,
                                                                    'ignore3'     ,
                                                                    'ignore4'     ,
                                                                    'optionType'  ,
                                                                    'DeltaFlag'   ,
                                                                    'ignore5'     ,
                                                                    'ignore6'     ,
                                                                    'ignore7'     ,
                                                                    'ignore8'     ,
                                                                    'ignore9'     ,
                                                                    'ignore10'    ,
                                                                    'ignore11'    ,
                                                                    'DeltaValue'  ,
                                                                    'tenor'       ,
                                                                    'volatility'  ] )

    filename = str_dateIdentifier + filenameSuffix + "." + fileFormat


    main_list = []
    df = pd.read_sql(sql_Exchange, cnxn)
    main_list.extend(df.values.tolist())
    df = pd.read_sql(sql_FXATMVol, cnxn)
    main_list.extend(df.values.tolist())
    df = pd.read_sql(sql_Yield, cnxn)
    main_list.extend(df.values.tolist())
    df = pd.read_sql(sql_FXDeltaVolMtx, cnxn)
    main_list.extend(df.values.tolist())

#     print(len(main_list))
    Exc = []
    Yld = []
    FXAtm = []
    FXDelta = []
    for line in main_list:
        line[0] = line[0].replace("\"", "")
        if line[0] == "Exchange":
            if len(line) != 5:
                print('[ERROR - readMarketData]. Could not read Exchange data, expecting 5 columns')
                print('    --------- Aborting ----------')
                completedNoError=False
                return completedNoError, \
                    df_CurrentMarketData_Exchange, \
                    df_CurrentMarketData_Yield, \
                    df_CurrentMarketData_FXATMVol, \
                    df_CurrentMarketData_FXDeltaVol

            dict_Exchange = dict()
            dict_Exchange["type"]    = line[0].replace("\"", "").replace("\'", "")
            dict_Exchange["ccy_FOR"] = line[1].replace("\"", "").replace("\'", "")
            dict_Exchange["date"]    = line[2].replace("-", "/").replace("\"", "").replace("\'", "").split(" ")[0]
            dict_Exchange["ccy_DOM"] = line[3].replace("\"", "").replace("\'", "")
            dict_Exchange["rate"]    = float(line[4].replace("\"", "").replace("\'", ""))
            Exc.append(dict_Exchange)


        #---------------------------------------------------------------------#
        #          READ YIELD DATA                                            #
        #---------------------------------------------------------------------#

        elif line[0] == "Yield":
            if len(line) != 10:
                print('[ERROR - readMarketData]. Could not read Yield data, expecting 10 columns')
                print('    --------- Aborting ----------')
                completedNoError=False
                return completedNoError, \
                    df_CurrentMarketData_Exchange, \
                    df_CurrentMarketData_Yield, \
                    df_CurrentMarketData_FXATMVol, \
                    df_CurrentMarketData_FXDeltaVol

            dict_Yield = dict()
            dict_Yield["type"]      = line[0].replace("\"", "").replace("\'", "")
            dict_Yield["curvename"] = line[1].replace("\"", "").replace("\'", "")
            dict_Yield["date"]      = line[2].replace("-", "/").replace("\"", "").replace("\'", "").split(" ")[0]
            dict_Yield["ccy_yield"] = line[3].replace("\"", "").replace("\'", "")
            dict_Yield["ignore1"]   = line[4].replace("\"", "").replace("\'", "")
            dict_Yield["ignore2"]   = line[5].replace("\"", "").replace("\'", "")
            dict_Yield["ignore3"]   = line[6].replace("\"", "").replace("\'", "")
            dict_Yield["ignore4"]   = line[7].replace("\"", "").replace("\'", "")
            dict_Yield["tenor"]     = np.float64(line[8].replace("\"", "").replace("\'", ""))
            dict_Yield["yield"]     = np.float64(line[9].replace("\"", "").replace("\'", ""))
            Yld.append(dict_Yield)
            #df_CurrentMarketData_Yield = df_CurrentMarketData_Yield.append(dict_Yield, ignore_index=True)

        #---------------------------------------------------------------------#
        #          READ FX ATM VOLATILITY DATA                                #
        #---------------------------------------------------------------------#
        elif line[0] == "FXATMVol":
            if len(line) != 16:
                print('[ERROR - readMarketData]. Could not read FXATMVol data, expecting 16 columns')
                print('    --------- Aborting ----------')
                completedNoError=False
                return completedNoError, \
                    df_CurrentMarketData_Exchange, \
                    df_CurrentMarketData_Yield, \
                    df_CurrentMarketData_FXATMVol, \
                    df_CurrentMarketData_FXDeltaVol

            dict_ATMVol = dict()
            dict_ATMVol['type']        = line[0]
            dict_ATMVol['curvename']   = line[1].replace("\"", "").replace("\'", "")
            dict_ATMVol['date']        = line[2].replace("-", "/").replace("\"", "").replace("\'", "").split(" ")[0]
            dict_ATMVol['ccy_counter'] = line[3].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore1']     = line[4].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore2']     = line[5].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore3']     = line[6].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore4']     = line[7].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore5']     = line[8].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore6']     = line[9].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore7']     = line[10].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore8']     = line[11].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore9']     = line[12].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore10']    = line[13].replace("\"", "").replace("\'", "")
            dict_ATMVol['tenor']       = np.float64(line[14].replace("\"", "").replace("\'", ""))
            dict_ATMVol['volatility']  = np.float64(line[15].replace("\"", "").replace("\'", ""))
            FXAtm.append(dict_ATMVol)

            #df_CurrentMarketData_FXATMVol = df_CurrentMarketData_FXATMVol.append(dict_ATMVol, ignore_index=True)


        #---------------------------------------------------------------------#
        #          READ FX DELTA VOLATILITY DATA                              #
        #---------------------------------------------------------------------#

        elif line[0] == "FXDeltaVolMtx":
            if len(line) != 20:
                print('[ERROR - readMarketData]. Could not read FXDeltaVolMtx data, expecting 20 columns')
                print('    --------- Aborting ----------')
                completedNoError=False
                return completedNoError, \
                    df_CurrentMarketData_Exchange, \
                    df_CurrentMarketData_Yield, \
                    df_CurrentMarketData_FXATMVol, \
                    df_CurrentMarketData_FXDeltaVol

            dict_DeltaVol = dict()
            dict_DeltaVol['type']        = line[0].replace("\"", "").replace("\'", "")
            dict_DeltaVol['curvename']   = line[1].replace("\"", "").replace("\'", "")
            dict_DeltaVol['date']        = line[2].replace("-", "/").replace("\"", "").replace("\'", "").split(" ")[0]
            dict_DeltaVol['ccy_counter'] = line[3].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore1']     = line[4].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore2']     = line[5].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore3']     = line[6].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore4']     = line[7].replace("\"", "").replace("\'", "")
            dict_DeltaVol['optionType']  = line[8].replace("\"", "").replace("\'", "")
            dict_DeltaVol['DeltaFlag']   = line[9].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore5']     = line[10].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore6']     = line[11].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore7']     = line[12].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore8']     = line[13].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore9']     = line[14].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore10']    = line[15].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore11']    = line[16].replace("\"", "").replace("\'", "")
            dict_DeltaVol['DeltaValue']  = np.float64(line[17].replace("\"", "").replace("\'", ""))
            dict_DeltaVol['tenor']       = np.float64(line[18].replace("\"", "").replace("\'", ""))
            dict_DeltaVol['volatility']  = np.float64(line[19].replace("\"", "").replace("\'", ""))
            FXDelta.append(dict_DeltaVol)
            #df_CurrentMarketData_FXDeltaVol = df_CurrentMarketData_FXDeltaVol.append(dict_DeltaVol, ignore_index=True)

        else:
            print('[ERROR - readCurrentMarketData]. Unknown MarketData type:')
            print(line)
            completedNoError=False
            return completedNoError, \
                df_CurrentMarketData_Exchange, \
                df_CurrentMarketData_Yield, \
                df_CurrentMarketData_FXATMVol, \
                df_CurrentMarketData_FXDeltaVol

    df_CurrentMarketData_Exchange = df_CurrentMarketData_Exchange.append(Exc, ignore_index=False)
    df_CurrentMarketData_Yield = df_CurrentMarketData_Yield.append(Yld, ignore_index=False)
    df_CurrentMarketData_FXATMVol = df_CurrentMarketData_FXATMVol.append(FXAtm, ignore_index=False)
    df_CurrentMarketData_FXDeltaVol = df_CurrentMarketData_FXDeltaVol.append(FXDelta, ignore_index=False)

    completedNoError=True
#     print(df_CurrentMarketData_Yield.head())
    return completedNoError, \
        df_CurrentMarketData_Exchange, \
        df_CurrentMarketData_Yield, \
        df_CurrentMarketData_FXATMVol,\
        df_CurrentMarketData_FXDeltaVol
###############################################################################
#        READ HISTORICAL MARKETDATA                                           #
###############################################################################
def readHistoricalMarketData( str_dateIdentifier                              ,
                              filenameSuffix    = "-BARC-HistoricalMarketData",
                              fileFormat        = "csv"                       ,
                              list_typesToRead  = ["Exchange"]                ):
    """
    Read in HistoricalMarketData.csv as it comes from MDM process.
      [1] Split it into four sections: Exchange, Yield, FXDeltaVol, FXATMVol
      [2] Read only the data that is listed in list_typesToRead

    Return value: five values:
      1       ... completedNoError [True/False]
      2,3,4,5 ... DataFrames containing MarketData
    """
    db_name = os.environ.get('DB_NAME')
    pwd = os.environ.get('SA_PASSWORD')
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.0.75.1,1433;DATABASE=rh01;UID=sa;PWD=qbasik007;TDS_Version=8.0;'

    cnxn = pyodbc.connect(
                         # r'DRIVER={FreeTDS};'
                         # r'SERVER=DESKTOP-8CBSCDJ;'
                         # r'DATABASE=rh01;'
                         # r'Trusted_Connection=yes;'
                         connection_string
                         )
    #create cursor
    cursor = cnxn.cursor()

    #EQUITY Query
    sql_Equity        = """select ["type"],
                                 ["isin"],
                                 ["closepricedate"],
                                 ["pricecurrencyid"],
                                 ["closeprice"]
                        from [dbo].[mdm.price]
                        where type='Equity'"""
    #EXCHANGE Query
    sql_Exchange      = """select 'Exchange' as 'Exchange',
                                 ["curveID"],
                                 ["observationDate"],
                                 ["currencyID"],
                                 ["exchangeRate"]
                         from [dbo].[mdm.rate]"""
    #FXAtmVol Query
    sql_FXATMVol      = """select ["curvetype"],
                                 ["curveID"] + '-' + 'ATM' as 'curveID',
                                 ["observationDate"],
                                 ["currencyID"],
                                 'lin' as 'lin',
                                 'near' as 'near',
                                 ["atmType"],
                                 ["deltaPremium"],
                                 ["daysPerAnnum"],
                                 ["holidaySet"],
                                 ["expiryUnits"],
                                 ["busDayConv"],
                                 ["invalidDateRule"],
                                 ["endOfMonthRule"],
                                 ["expiry"],
                                 ["vol"]
                        from [dbo].[mdm.surfaceATM]"""
    #FXNonAtmVol Query
    sql_FXDeltaVolMtx = """select ["curvetype"],
                                     ["curveID"] + '-' + ["quoteInstrumentType"] as 'curveID',
                                     ["observationDate"],
                                     ["currencyID"],
                                      'lin' as 'lin',
                                      'near' as 'near',
                                      'lin' as 'lin',
                                      'near' as 'near',
                                     ["quoteInstrumentType"],
                                     ["deltaType"],
                                     ["deltaPremium"],
                                     ["daysPerAnnum"],
                                     ["holidaySet"],
                                     ["expiryUnits"],
                                     ["busDayConv"],
                                     ["invalidDateRule"],
                                     ["endOfMonthRule"],
                                     ["delta"],
                                     ["expiry"],
                                     ["vol"]
                             from [dbo].[mdm.surfaceOTM]"""
    #Yield Curves Query
    sql_Yield        = """select 'Yield' as 'Yield',
                                  [dbo].[mdm.curve].["curveID"],
                                  [dbo].[mdm.curve].["observationDate"],
                                  [dbo].[mdm.curve].["currencyID"],
                                  'lin' as 'lin',
                                  'near' as 'near',
                                  'ACT365' as 'ACT365',
                                  'DAILY' as 'DAILY',
                                  [dbo].[mdm.curvepoint].["maturity"],
                                  [dbo].[mdm.curvepoint].["yield"]
                        from
                        [dbo].[mdm.curve] inner join [dbo].[mdm.curvepoint]
                        on [dbo].[mdm.curve].["ID"]=[dbo].[mdm.curvepoint].["ID"]"""



    df_HistoricalMarketData_Exchange     = pd.DataFrame( columns = [ 'type'    ,
                                                                  'ccy_FOR' ,
                                                                  'date'    ,
                                                                  'ccy_DOM' ,
                                                                  'rate'    ] )

    df_HistoricalMarketData_Yield        = pd.DataFrame( columns = [ 'type'      ,
                                                                  'curvename' ,
                                                                  'date'      ,
                                                                  'ccy_yield' ,
                                                                  'ignore1'   ,
                                                                  'ignore2'   ,
                                                                  'ignore3'   ,
                                                                  'ignore4'   ,
                                                                  'tenor'     ,
                                                                  'yield'     ] )

    df_HistoricalMarketData_FXATMVol     = pd.DataFrame( columns = [ 'type'        ,
                                                                  'curvename'   ,
                                                                  'date'        ,
                                                                  'ccy_counter' ,
                                                                  'ignore1'     ,
                                                                  'ignore2'     ,
                                                                  'ignore3'     ,
                                                                  'ignore4'     ,
                                                                  'ignore5'     ,
                                                                  'ignore6'     ,
                                                                  'ignore7'     ,
                                                                  'ignore8'     ,
                                                                  'ignore9'     ,
                                                                  'ignore10'    ,
                                                                  'tenor'       ,
                                                                  'volatility'  ] )

    df_HistoricalMarketData_FXDeltaVol     = pd.DataFrame( columns = [ 'type'        ,
                                                                    'curvename'   ,
                                                                    'date'        ,
                                                                    'ccy_counter' ,
                                                                    'ignore1'     ,
                                                                    'ignore2'     ,
                                                                    'ignore3'     ,
                                                                    'ignore4'     ,
                                                                    'optionType'  ,
                                                                    'DeltaFlag'   ,
                                                                    'ignore5'     ,
                                                                    'ignore6'     ,
                                                                    'ignore7'     ,
                                                                    'ignore8'     ,
                                                                    'ignore9'     ,
                                                                    'ignore10'    ,
                                                                    'ignore11'    ,
                                                                    'DeltaValue'  ,
                                                                    'tenor'       ,
                                                                    'volatility'  ] )

    filename = str_dateIdentifier + filenameSuffix + "." + fileFormat


    main_list = []
    df = pd.read_sql(sql_Exchange, cnxn)
    main_list.extend(df.values.tolist())
    df = pd.read_sql(sql_FXATMVol, cnxn)
    main_list.extend(df.values.tolist())
    df = pd.read_sql(sql_Yield, cnxn)
    main_list.extend(df.values.tolist())
    df = pd.read_sql(sql_FXDeltaVolMtx, cnxn)
    main_list.extend(df.values.tolist())

#     print(len(main_list))
    Exc = []
    Yld = []
    FXAtm = []
    FXDelta = []


    for line in main_list:
        line[0] = line[0].replace("\"", "")
        if line[0] == "Exchange":
            if len(line) != 5:
                print('[ERROR - readMarketData]. Could not read Exchange data, expecting 5 columns')
                print('    --------- Aborting ----------')
                completedNoError=False
                return completedNoError, \
                    df_HistoricalMarketData_Exchange, \
                    df_HistoricalMarketData_Yield, \
                    df_HistoricalMarketData_FXATMVol, \
                    df_HistoricalMarketData_FXDeltaVol

            dict_Exchange = dict()
            dict_Exchange["type"]    = line[0].replace("\"", "").replace("\'", "")
            dict_Exchange["ccy_FOR"] = line[1].replace("\"", "").replace("\'", "")
            dict_Exchange["date"]    = line[2].replace("-", "/").replace("\"", "").replace("\'", "").split(" ")[0]
            dict_Exchange["ccy_DOM"] = line[3].replace("\"", "").replace("\'", "")
            dict_Exchange["rate"]    = float(line[4].replace("\"", "").replace("\'", ""))
            Exc.append(dict_Exchange)


        #---------------------------------------------------------------------#
        #          READ YIELD DATA                                            #
        #---------------------------------------------------------------------#

        elif line[0] == "Yield":
            if len(line) != 10:
                print('[ERROR - readMarketData]. Could not read Yield data, expecting 10 columns')
                print('    --------- Aborting ----------')
                completedNoError=False
                return completedNoError, \
                    df_HistoricalMarketData_Exchange, \
                    df_HistoricalMarketData_Yield, \
                    df_HistoricalMarketData_FXATMVol, \
                    df_HistoricalMarketData_FXDeltaVol

            dict_Yield = dict()
            dict_Yield["type"]      = line[0].replace("\"", "").replace("\'", "")
            dict_Yield["curvename"] = line[1].replace("\"", "").replace("\'", "")
            dict_Yield["date"]      = line[2].replace("-", "/").replace("\"", "").replace("\'", "").split(" ")[0]
            dict_Yield["ccy_yield"] = line[3].replace("\"", "").replace("\'", "")
            dict_Yield["ignore1"]   = line[4].replace("\"", "").replace("\'", "")
            dict_Yield["ignore2"]   = line[5].replace("\"", "").replace("\'", "")
            dict_Yield["ignore3"]   = line[6].replace("\"", "").replace("\'", "")
            dict_Yield["ignore4"]   = line[7].replace("\"", "").replace("\'", "")
            dict_Yield["tenor"]     = np.float64(line[8].replace("\"", "").replace("\'", ""))
            dict_Yield["yield"]     = np.float64(line[9].replace("\"", "").replace("\'", ""))
            Yld.append(dict_Yield)
            #df_CurrentMarketData_Yield = df_CurrentMarketData_Yield.append(dict_Yield, ignore_index=True)

        #---------------------------------------------------------------------#
        #          READ FX ATM VOLATILITY DATA                                #
        #---------------------------------------------------------------------#
        elif line[0] == "FXATMVol":
            if len(line) != 16:
                print('[ERROR - readMarketData]. Could not read FXATMVol data, expecting 16 columns')
                print('    --------- Aborting ----------')
                completedNoError=False
                return completedNoError, \
                    df_HistoricalMarketData_Exchange, \
                    df_HistoricalMarketData_Yield, \
                    df_HistoricalMarketData_FXATMVol, \
                    df_HistoricalMarketData_FXDeltaVol

            dict_ATMVol = dict()
            dict_ATMVol['type']        = line[0]
            dict_ATMVol['curvename']   = line[1].replace("\"", "").replace("\'", "")
            dict_ATMVol['date']        = line[2].replace("-", "/").replace("\"", "").replace("\'", "").split(" ")[0]
            dict_ATMVol['ccy_counter'] = line[3].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore1']     = line[4].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore2']     = line[5].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore3']     = line[6].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore4']     = line[7].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore5']     = line[8].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore6']     = line[9].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore7']     = line[10].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore8']     = line[11].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore9']     = line[12].replace("\"", "").replace("\'", "")
            dict_ATMVol['ignore10']    = line[13].replace("\"", "").replace("\'", "")
            dict_ATMVol['tenor']       = np.float64(line[14].replace("\"", "").replace("\'", ""))
            dict_ATMVol['volatility']  = np.float64(line[15].replace("\"", "").replace("\'", ""))
            FXAtm.append(dict_ATMVol)

            #df_CurrentMarketData_FXATMVol = df_CurrentMarketData_FXATMVol.append(dict_ATMVol, ignore_index=True)


        #---------------------------------------------------------------------#
        #          READ FX DELTA VOLATILITY DATA                              #
        #---------------------------------------------------------------------#

        elif line[0] == "FXDeltaVolMtx":
            if len(line) != 20:
                print('[ERROR - readMarketData]. Could not read FXDeltaVolMtx data, expecting 20 columns')
                print('    --------- Aborting ----------')
                completedNoError=False
                return completedNoError, \
                    df_HistoricalMarketData_Exchange, \
                    df_HistoricalMarketData_Yield, \
                    df_HistoricalMarketData_FXATMVol, \
                    df_HistoricalMarketData_FXDeltaVol

            dict_DeltaVol = dict()
            dict_DeltaVol['type']        = line[0].replace("\"", "").replace("\'", "")
            dict_DeltaVol['curvename']   = line[1].replace("\"", "").replace("\'", "")
            dict_DeltaVol['date']        = line[2].replace("-", "/").replace("\"", "").replace("\'", "").split(" ")[0]
            dict_DeltaVol['ccy_counter'] = line[3].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore1']     = line[4].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore2']     = line[5].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore3']     = line[6].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore4']     = line[7].replace("\"", "").replace("\'", "")
            dict_DeltaVol['optionType']  = line[8].replace("\"", "").replace("\'", "")
            dict_DeltaVol['DeltaFlag']   = line[9].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore5']     = line[10].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore6']     = line[11].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore7']     = line[12].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore8']     = line[13].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore9']     = line[14].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore10']    = line[15].replace("\"", "").replace("\'", "")
            dict_DeltaVol['ignore11']    = line[16].replace("\"", "").replace("\'", "")
            dict_DeltaVol['DeltaValue']  = np.float64(line[17].replace("\"", "").replace("\'", ""))
            dict_DeltaVol['tenor']       = np.float64(line[18].replace("\"", "").replace("\'", ""))
            dict_DeltaVol['volatility']  = np.float64(line[19].replace("\"", "").replace("\'", ""))
            FXDelta.append(dict_DeltaVol)
            #df_CurrentMarketData_FXDeltaVol = df_CurrentMarketData_FXDeltaVol.append(dict_DeltaVol, ignore_index=True)

        else:
            print('[ERROR - readCurrentMarketData]. Unknown MarketData type:')
            print(line)
            completedNoError=False
            return completedNoError, \
                df_HistoricalMarketData_Exchange, \
                df_HistoricalMarketData_Yield, \
                df_HistoricalMarketData_FXATMVol, \
                df_HistoricalMarketData_FXDeltaVol

    df_HistoricalMarketData_Exchange = df_HistoricalMarketData_Exchange.append(Exc, ignore_index=False)
    df_HistoricalMarketData_Yield = df_HistoricalMarketData_Yield.append(Yld, ignore_index=False)
    df_HistoricalMarketData_FXATMVol = df_HistoricalMarketData_FXATMVol.append(FXAtm, ignore_index=False)
    df_HistoricalMarketData_FXDeltaVol = df_HistoricalMarketData_FXDeltaVol.append(FXDelta, ignore_index=False)


    completedNoError=True
#     print(df_CurrentMarketData_Yield.head())
    return completedNoError, \
        df_HistoricalMarketData_Exchange, \
        df_HistoricalMarketData_Yield, \
        df_HistoricalMarketData_FXATMVol, \
        df_HistoricalMarketData_FXDeltaVol
 ###############################################################################
#        READ SETTINGS FILE                                                   #
###############################################################################
def readSettings(filename="Settings.json"):
    script_dir = os.path.dirname(__file__)
    with open(os.path.join(script_dir, filename)) as file_Settings:
        dict_settingsInfo = json.load(file_Settings)

    #-------------------------------------------------------------------------#
    # check for data completeness                                             #
    #-------------------------------------------------------------------------#
    if "tradeDate" not in dict_settingsInfo:
        print('[ERROR]. The keyword tradeDate is missing in the SettingsFile ', filename)
        print('---------- Aborting Run ----------')
        return None
    else:
        list_date = dict_settingsInfo["tradeDate"].split("/")

        if (len(list_date) != 3)    or \
           (int(list_date[0]) < 1)  or \
           (int(list_date[0]) > 31) or \
           (int(list_date[1]) < 1)  or \
           (int(list_date[1]) > 12) or \
           (len(list_date[0]) != 2) or \
           (len(list_date[1]) != 2) or \
           (len(list_date[2]) != 4):
            print('[ERROR]. The tradeDate format is wrong.')
            print('Required: DD/MM/YYYY')
            print('Found: ', dict_settingsInfo["tradeDate"])
            print('---------- Aborting Run ----------')
            return None

    if "randomSeed" not in dict_settingsInfo:
        print('[ERROR]. The keyword randomSeed is missing in the SettingsFile ', filename)
        print('---------- Aborting Run ----------')
        return None

    if "nPRIIPsSimulations" not in dict_settingsInfo:
        print('[ERROR]. The keyword nPRIIPsSimulations is missing in the SettingsFile ', filename)
        print('         The default value should be 10000')
        print('---------- Aborting Run ----------')
        return None

    if "filename_ProductList" not in dict_settingsInfo:
        print('[ERROR]. The keyword filename_ProductList is missing in the SettingsFile ', filename)
        print('---------- Aborting Run ----------')
        return None

    if "readRandomIndices" not in dict_settingsInfo:
        print('[ERROR]. The keyword readRandomIndices is missing in the SettingsFile ', filename)
        print('---------- Aborting Run ----------')
        return None
    else:
        if dict_settingsInfo["readRandomIndices"] == True:
            if "readRandomIndicesFilename" not in dict_settingsInfo:
                print('[ERROR]. The keyword readRandomIndices=true needs the keyword readRandomIndicesFilename ')
                print('---------- Aborting Run ----------')
                return None

    if "writePRIIPsPaths" not in dict_settingsInfo:
        print('[ERROR]. The keyword writePRIIPsPaths is missing in the SettingsFile ', filename)
        print('---------- Aborting Run ----------')
        return None

    if "writeRandomIndices" not in dict_settingsInfo:
        print('[ERROR]. The keyword writeRandomIndices is missing in the SettingsFile ', filename)
        print('---------- Aborting Run ----------')
        return None

    if "writeProductLog"  not in dict_settingsInfo:
        print('[ERROR]. The keyword writeProductLog is missing in the SettingsFile ', filename)
        print('---------- Aborting Run ----------')
        return None

    if "filename_Mapping"  not in dict_settingsInfo:
        print('[ERROR]. The keyword filename_Mapping is missing in the SettingsFile ', filename)
        print('---------- Aborting Run ----------')
        return None

    else:
        if dict_settingsInfo["writeProductLog"] == True:
            if "writeProductLogFilename" not in dict_settingsInfo:
                print('[ERROR]. The keyword writeProductLogFilename=true needs the keyword writeProductLogFilename ')
                print('---------- Aborting Run ----------')
                return None

    if "writePayoffVector"  not in dict_settingsInfo:
        print('[ERROR]. The keyword writePayoffVector is missing in the SettingsFile ', filename)
        print('---------- Aborting Run ----------')
        return None

    return dict_settingsInfo

###############################################################################
#        READ MAPPING FILE                                                    #
###############################################################################
def readMapping(filename="Mapping.json"):
    script_dir = os.path.dirname(__file__)
    with open(os.path.join(script_dir, filename)) as file_Mapping:
        dict_Mapping = json.load(file_Mapping)

        return dict_Mapping

###############################################################################
#        READ PRODUCT SCOPE FILE                                              #
###############################################################################
def read_setup_products(filename="list_ProductsToRun.json"):
    script_dir = os.path.dirname(__file__)
    with open(os.path.join(script_dir, filename)) as file_ProductsToRun:
        data = json.load(file_ProductsToRun)

        return data

###############################################################################
#        READ PRIIPs paths                                                    #
###############################################################################
def read_PRIIPsPath(filename):
    script_dir = os.path.dirname(__file__)
    df_paths = pd.read_csv(os.path.join(script_dir, filename), sep=',', header=None, index_col=False)
    return df_paths
