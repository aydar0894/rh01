def run_workflow(input):

    import pandas as pd
    import numpy as np
    import random
    import sys
    import os.path
    import timeit
    import json

    from .lib.readData import readSettings                   , \
                             readMapping                    , \
                             readRandomIndexFile            , \
                             readCurrentMarketData          , \
                             readHistoricalMarketData       , \
                             read_PRIIPsPath

    from .lib.setup_products import setup_products

    from .lib.PreProcessing import get_SpotRate         , \
                                  getStrDateIdentifier

    from .lib.Bootstrap import calculate_LogReturns_FX       , \
                              read_RandomIndices            , \
                              select_RandomIndices          , \
                              calculate_ItoTerm             , \
                              calculate_ShiftTerm           , \
                              calculate_PRIIPsPath_FMU      , \
                              calculate_PRIIPsPath_Stressed , \
                              calculate_stressed_vol

    from .lib.LogInformation import getAllClassAttributes         , \
                                   write_LogInfo_ClassAttributes , \
                                   get_REO_columnOrder           , \
                                   write_AttributesFile


    ###############################################################################
    #        SET UP TIMER                                                         #
    ###############################################################################
    dict_timer = dict()

    dict_timer['start'] = timeit.default_timer()


    ###############################################################################
    #        READ GENERIC PARAMETERS FROM SETTINGS FILE                           #
    ###############################################################################
    print('')
    print('[READING SETTINGS FILE]')
    dict_settings = readSettings(filename="Settings_20180625.json")
    if dict_settings == None:
        sys.exit()

    str_dateIdentifier = getStrDateIdentifier(dict_settings["tradeDate"])
    if str_dateIdentifier == None:
        sys.exit()

    # add timer
    dict_timer['readSettings'] = timeit.default_timer()

    ###############################################################################
    #        SETUP PRODUCTS                                                       #
    ###############################################################################
    print('')
    print('[INITIALIZING PRODUCT LIST]')
    # list_products = setup_products(filename=dict_settings['filename_ProductList'])
    jsonObj = input
    list_products = setup_products(jsonObj)
    # add timer
    dict_timer['setupProducts'] = timeit.default_timer()

    ###############################################################################
    #        DETECT UNDERLYERS                                                    #
    ###############################################################################
    print('')
    print('[DETECTED THE FOLLOWING UNDERLYERS]')

    list_FX_rates            = []
    list_FX_rates_toSimulate = []
    list_FX_rates_toReadPath = []

    for product in list_products:
        FX_rate = product.ccy_FOR + product.ccy_DOM

        if FX_rate not in list_FX_rates:
            list_FX_rates.append(FX_rate)

    for FX_rate in list_FX_rates:
        script_dir = os.path.dirname(__file__)
        filename_FMU = os.path.join(script_dir + "\\lib\\", str_dateIdentifier + "-PATH_FMU_" + FX_rate + ".csv")
        filename_S   = os.path.join(script_dir + "\\lib\\", str_dateIdentifier + "-PATH_FMU_" + FX_rate + ".csv")
        print(os.path.isfile(filename_FMU))
        if os.path.isfile(filename_FMU) and os.path.isfile(filename_S):
            print('  (+) ' + FX_rate + '. PRIIPs paths exist already' )
            list_FX_rates_toReadPath.append(FX_rate)
        else:
            print('  (+) ' + FX_rate + '. PRIIPs paths do not exist yet' )
            list_FX_rates_toSimulate.append(FX_rate)

    # add timer
    dict_timer['detectUnderlyings'] = timeit.default_timer()

    ###############################################################################
    #        READ MARKET DATA                                                     #
    ###############################################################################
    print('')
    print('[READING MARKET DATA]')

    #-----------------------------------------------------------------------------#
    #             Current Market Data                                             #
    #-----------------------------------------------------------------------------#
    print('   (+) Current Market Data ...')
    completedNoError                , \
    df_CurrentMarketData_Exchange   , \
    df_CurrentMarketData_Yield      , \
    df_CurrentMarketData_FXATMVol   , \
    df_CurrentMarketData_FXDeltaVol = readCurrentMarketData( str_dateIdentifier = str_dateIdentifier,
                                                              filenameSuffix     = "-BARC-CurrentMarketData",
                                                              fileFormat         = "csv")

    # if there was just one error reading the files --> abort run
    if completedNoError == False:
        sys.exit()

    # add timer
    dict_timer['readCurrentMarketData'] = timeit.default_timer()

    #-----------------------------------------------------------------------------#
    #             Historical Market Data                                          #
    #-----------------------------------------------------------------------------#
    if len(list_FX_rates_toSimulate) != 0:
        print('   (+) Historical Market Data ...')

        completedNoError                   , \
        df_HistoricalMarketData_Exchange   , \
        df_HistoricalMarketData_Yield      , \
        df_HistoricalMarketData_FXATMVol   , \
        df_HistoricalMarketData_FXDeltaVol = readHistoricalMarketData( str_dateIdentifier = str_dateIdentifier,
                                                                       filenameSuffix     = "-BARC-HistoricalMarketData",
                                                                       fileFormat         = "csv",
                                                                       list_typesToRead   = ["Exchange"] )

        # if there was just one error reading the files (all 4 are None) --> abort run
        if completedNoError == False:
            sys.exit()

    # add timer
    dict_timer['readHistoricalMarketData'] = timeit.default_timer()

    #-----------------------------------------------------------------------------#
    #             Read Random Indices                                             #
    #-----------------------------------------------------------------------------#
    if dict_settings['readRandomIndices'] == True:
        df_RandomIndices = readRandomIndexFile(filename = dict_settings["readRandomIndicesFilename"])

    # add timer
    dict_timer['readRandomIndices'] = timeit.default_timer()

    ###############################################################################
    #        READ MAPPINGS: CurrencyName to CurveName                             #
    ###############################################################################
    dict_Mapping = readMapping(filename=dict_settings["filename_Mapping"])

    # add timer
    dict_timer['readMappings'] = timeit.default_timer()

    ###############################################################################
    #        PRE-PROCESSING                                                       #
    ###############################################################################
    print('')
    print('[RUNNING PRE-PROCESSING]')
    for product_id, product in enumerate(list_products):
        # set product id
        product.set_productID(product_id)

        # set tradeDate
        product.set_tradeDate(dict_settings['tradeDate'])

        errorMessage = 0

        #-------------------------------------------------------------------------#
        #         FX Forward                                                      #
        #-------------------------------------------------------------------------#
        if product.product_type == 'FX_Forward':
            errorMessage = product.preProcessing(df_CurrentMarketData_Exchange = df_CurrentMarketData_Exchange ,
                                                 df_CurrentMarketData_Yield    = df_CurrentMarketData_Yield    ,
                                                 dict_YieldCcy_CurveName       = dict_Mapping["FX_Yield"]      )

        #-------------------------------------------------------------------------#
        #         FX Swap                                                         #
        #-------------------------------------------------------------------------#
        elif product.product_type == 'FX_Swap':
            errorMessage = product.preProcessing(df_CurrentMarketData_Exchange = df_CurrentMarketData_Exchange ,
                                                 df_CurrentMarketData_Yield    = df_CurrentMarketData_Yield    ,
                                                 dict_YieldCcy_CurveName       = dict_Mapping["FX_Yield"]      )

        #-------------------------------------------------------------------------#
        #         FX Option                                                       #
        #-------------------------------------------------------------------------#
        elif product.product_type == 'FX_Option':
            errorMessage = product.preProcessing(df_CurrentMarketData_Exchange = df_CurrentMarketData_Exchange ,
                                                 df_CurrentMarketData_Yield    = df_CurrentMarketData_Yield    ,
                                                 dict_YieldCcy_CurveName       = dict_Mapping["FX_Yield"]      ,
                                                 df_CurrentMarketData_FXATMVol = df_CurrentMarketData_FXATMVol ,
                                                 dict_FXATMVol_CurveName       = dict_Mapping["FX_Vol_ATM"]    )

        #-------------------------------------------------------------------------#
        #         FX DCI                                                          #
        #-------------------------------------------------------------------------#
        elif product.product_type == 'FX_DCI':
            errorMessage = product.preProcessing(df_CurrentMarketData_Exchange   = df_CurrentMarketData_Exchange   ,
                                                 df_CurrentMarketData_Yield      = df_CurrentMarketData_Yield      ,
                                                 dict_YieldCcy_CurveName         = dict_Mapping["FX_Yield"]        ,
                                                 df_CurrentMarketData_FXATMVol   = df_CurrentMarketData_FXATMVol   ,
                                                 dict_FXATMVol_CurveName         = dict_Mapping["FX_Vol_ATM"]      ,
                                                 df_CurrentMarketData_FXDeltaVol = df_CurrentMarketData_FXDeltaVol ,
                                                 dict_FXDeltaVol_MS_CurveName    = dict_Mapping["FX_Vol_DeltaMS"]  ,
                                                 dict_FXDeltaVol_RR_CurveName    = dict_Mapping["FX_Vol_DeltaRR"]  )

        #-------------------------------------------------------------------------#
        #         FX ODF                                                          #
        #-------------------------------------------------------------------------#
        elif product.product_type == 'FX_ODF':
            errorMessage = product.preProcessing(df_CurrentMarketData_Exchange = df_CurrentMarketData_Exchange ,
                                                 df_CurrentMarketData_Yield    = df_CurrentMarketData_Yield    ,
                                                 dict_YieldCcy_CurveName       = dict_Mapping["FX_Yield"]      )

        #-------------------------------------------------------------------------#
        #         unknown product type                                            #
        #-------------------------------------------------------------------------#
        else:
            print('[ERROR]. Product type ' + product.product_type + ' not known...')
            print('         Skipping')

            product.set_ErrorMessage()

            continue

        if errorMessage < 0:
            print('[ERROR]. A problem was found in preProcessing.')
            print('         Product affected: ', product.product_type, ' ID: ', product.product_id)
            print('         This product will be skipped for all upcoming computations...')

    # add timer
    dict_timer['preProcessing'] = timeit.default_timer()

    ###############################################################################
    #         Identify products belonging to same FX_rate                         #
    ###############################################################################
    dict_FXpair_to_FXproduct = dict()

    for FX_rate in list_FX_rates:
        ccy_FOR = FX_rate[0:3]
        ccy_DOM = FX_rate[3:6]

        list_product_ids = []

        for product_id, product in enumerate(list_products):
            # skip products for which an error was raised
            if product.flag_error_encountered == True:
                continue

            if (product.ccy_FOR == ccy_FOR) and \
               (product.ccy_DOM == ccy_DOM):
                list_product_ids.append(product_id)

        dict_FXpair_to_FXproduct[FX_rate] = list_product_ids

    ###############################################################################
    #         Find product with the longest RHP period                            #
    ###############################################################################
    nTradingDaysRHP_max   = 0

    for product_id, product in enumerate(list_products):
        # [DEBUG] print('    product-id ', product_id, product.nTradingDaysRHP)

        if product.nTradingDaysRHP > nTradingDaysRHP_max:
            nTradingDaysRHP_max = product.nTradingDaysRHP

    # check if nTradingDaysMax is larger than #rows in RandomIndexFile
    if nTradingDaysRHP_max > df_RandomIndices.shape[0]:
        print('[WARNING]. The number of TradingDays is larger than the number of rows in RandomIndexFile.')
        print('           nTradingDaysRHP_max: ', nTradingDaysRHP_max)
        print('           number of rows in RandomIndexFile: ', df_RandomIndices.shape[0])

        if (nTradingDaysRHP_max - df_RandomIndices.shape[0] > 5):
            print('    [ERROR]. The difference between is large  (>5).')
            print('             Aborting run!')
            sys.exit()
        else:
            print('')
            print('           Will reset nTradingDaysRHP to match the number of rows')

            # reset all nTradingDays within 5 trading days
            for product_id, product in enumerate(list_products):
                if abs(product.nTradingDaysRHP - df_RandomIndices.shape[0]) < 5:
                    print('           Reset nTradingDaysRHP for product-id ', product_id, \
                                      ' from ', product.nTradingDaysRHP, ' to ', df_RandomIndices.shape[0])
                    list_products[product_id].nTradingDaysRHP = df_RandomIndices.shape[0]

    # add timer
    dict_timer['BookKeeping'] = timeit.default_timer()

    ###############################################################################
    #        MANUALLY OVERWRITE VALUES                                            #
    ###############################################################################
    # [ATTENTION]      This code block is here to help with manual debugging
    # [ATTENTION]      !!!!! It should be used very carefully !!!!!
    # [ATTENTION]
    # [ATTENTION]      Never use this in production environment, only for DEBUG!
    # [ATTENTION]

    # [NOPE] print('####################################################')
    # [NOPE] print('#      ATTENTION: MANUALLY OVERWRITING VALUES!     #')
    # [NOPE] print('####################################################')
    # [NOPE] for product_id, product in enumerate(list_products):
    # [NOPE]     #----------------------------------------------------------------#
    # [NOPE]     # 25/06/2018,  EURUSD, USDINR                                    #
    # [NOPE]     #----------------------------------------------------------------#
    # [NOPE]     # [X] if product.ccy_FOR == "EUR" and  product.ccy_DOM == "USD":
    # [NOPE]     # [X]     print(' !!! Overwriting Strike values for EURUSD !!! ')
    # [NOPE]     # [X]     product.strike = 1.19966
    # [NOPE]     # [X]
    # [NOPE]     # [X] if product.ccy_FOR == "USD" and  product.ccy_DOM == "INR":
    # [NOPE]     # [X]     print(' !!! Overwriting Strike values for USDINR !!! ')
    # [NOPE]     # [X]     product.strike = 71.3013
    # [NOPE]
    # [NOPE]     #----------------------------------------------------------------#
    # [NOPE]     # 26/06/2018,  EURUSD, USDINR                                    #
    # [NOPE]     #----------------------------------------------------------------#
    # [NOPE]     # [X] if product.ccy_FOR == "EUR" and  product.ccy_DOM == "USD":
    # [NOPE]     # [X]     print(' !!! Overwriting Strike values for EURUSD !!! ')
    # [NOPE]     # [X]     product.strike = 1.19753
    # [NOPE]     # [X]
    # [NOPE]     # [X] if product.ccy_FOR == "USD" and  product.ccy_DOM == "INR":
    # [NOPE]     # [X]     print(' !!! Overwriting Strike values for USDINR !!! ')
    # [NOPE]     # [X]     product.strike = 71.5467
    # [NOPE]
    # [NOPE]     #----------------------------------------------------------------#
    # [NOPE]     # 27/06/2018,  EURUSD, USDINR                                    #
    # [NOPE]     #----------------------------------------------------------------#
    # [NOPE]     # [X] if product.ccy_FOR == "EUR" and  product.ccy_DOM == "USD":
    # [NOPE]     # [X]     print(' !!! Overwriting Strike values for EURUSD !!! ')
    # [NOPE]     # [X]     product.strike = 1.1901
    # [NOPE]     # [X]
    # [NOPE]     # [X] if product.ccy_FOR == "USD" and  product.ccy_DOM == "INR":
    # [NOPE]     # [X]     print(' !!! Overwriting Strike values for USDINR !!! ')
    # [NOPE]     # [X]     product.strike = 71.8096

    ###############################################################################
    #        PRIIPs PATHS - SIMULATE                                              #
    ###############################################################################
    print('')
    print('[RUNNING PRIIPs BOOTSTRAP]')

    dict_paths_FMU = dict()
    dict_paths_S   = dict()

    for FX_rate in list_FX_rates_toSimulate:
        print('  (+) Simulating ' + FX_rate)

        df_underlyer = pd.DataFrame()

        #-------------------------------------------------------------------------#
        #    Get relevant market data                                             #
        #-------------------------------------------------------------------------#
        ccy_FOR = FX_rate[0:3]
        ccy_DOM = FX_rate[3:6]

        #.........................................................................#
        #         Spot rate                                                       #
        #.........................................................................#
        spot_rate = get_SpotRate(df_CurrentMarketData_Exchange = df_CurrentMarketData_Exchange, \
                                 ccy_FOR                       = ccy_FOR                      , \
                                 ccy_DOM                       = ccy_DOM                      )


        #.........................................................................#
        #         Log returns                                                     #
        #.........................................................................#
        arr_logReturns = calculate_LogReturns_FX( df_HistoricalMarketData_Exchange = df_HistoricalMarketData_Exchange, \
                                                  ccy_FOR                          = ccy_FOR                         , \
                                                  ccy_DOM                          = ccy_DOM                         )
        if arr_logReturns.size == 0:
            print('   Continue to next underlyer')
            continue

        #-------------------------------------------------------------------------#
        #    PRIIPs sampling - Favourable/Moderate/Unfavourable                   #
        #-------------------------------------------------------------------------#
        print('       (*) Favourable, Moderate, Unfavourable')

        if dict_settings['readRandomIndices'] == True:
            nTradingDaysRHP_max = df_RandomIndices.shape[0]

        # set random seed
        random.seed(dict_settings['randomSeed'])

        # calculate Ito term
        arr_ItoTerm = calculate_ItoTerm( arr_logReturns = arr_logReturns,
                                         n              = nTradingDaysRHP_max     )

        # calculate paths
        for i_resample in range(dict_settings['nPRIIPsSimulations']):
            if dict_settings['readRandomIndices'] == True:
                arr_idxSelected = read_RandomIndices( df_RandomIndices  = df_RandomIndices         ,
                                                      n_indices         = df_RandomIndices.shape[0],
                                                      rowSelection      = i_resample               ,
                                                      n_logReturns      = arr_logReturns.size      )
            else:
                arr_idxSelected = select_RandomIndices( n_indices = nTradingDaysRHP_max,
                                                        n_LogReturns = arr_logReturns.size)

            if arr_idxSelected.size == 0:
                print('   Continue to next underlyer')
                continue

            arr_underlying = calculate_PRIIPsPath_FMU( arr_randomIndices = arr_idxSelected ,
                                                       arr_logReturns    = arr_logReturns  ,
                                                       arr_ItoTerm       = arr_ItoTerm     ,
                                                       spot_rate         = spot_rate       )

            df_underlyer[i_resample] = arr_underlying
        dict_paths_FMU[FX_rate] = df_underlyer

        #-------------------------------------------------------------------------#
        #    PRIIPs sampling - Stressed                                           #
        #-------------------------------------------------------------------------#
        print('       (*) Stressed')

        df_underlyer = pd.DataFrame()

        if dict_settings['readRandomIndices'] == True:
            nTradingDaysRHP_max = df_RandomIndices.shape[0]

        # set random seed
        random.seed(dict_settings['randomSeed'])

        #TODO only <= 1Y products in scope, if onboarding of more products: UPDATE!
        stressed_vol = calculate_stressed_vol( arr_returns   = arr_logReturns, \
                                               window_length = 21              )

        # calculate rescaled returns
        arr_logReturnsRescaled = arr_logReturns * (stressed_vol / np.std(arr_logReturns))

        # calculate shift
        arr_shiftTerm = calculate_ShiftTerm( arr_logReturnsRescaled = arr_logReturnsRescaled,
                                             stressed_vol           = stressed_vol,
                                             n                      = nTradingDaysRHP_max)

        # calculate stressed paths
        for i_resample in range(dict_settings['nPRIIPsSimulations']):
            if dict_settings['readRandomIndices'] == True:
                arr_idxSelected = read_RandomIndices( df_RandomIndices  = df_RandomIndices            ,
                                                      n_indices         = df_RandomIndices.shape[0]   ,
                                                      rowSelection      = i_resample                  ,
                                                      n_logReturns      = arr_logReturnsRescaled.size )
            else:
                arr_idxSelected = select_RandomIndices( n_indices    = nTradingDaysRHP_max,
                                                        n_LogReturns = arr_logReturnsRescaled.size)

            if arr_idxSelected.size == 0:
                print('   Continue to next underlyer')
                continue

            arr_underlying = calculate_PRIIPsPath_Stressed( arr_randomIndices      = arr_idxSelected         ,
                                                            arr_logReturnsRescaled = arr_logReturnsRescaled  ,
                                                            arr_shiftTerm          = arr_shiftTerm           ,
                                                            spot_rate              = spot_rate               )

            df_underlyer[i_resample] = arr_underlying

        # [DEBUG] print('Memory: ' , df_underlyer.memory_usage().sum()/1e6 , ' MB')
        dict_paths_S[FX_rate] = df_underlyer

    ###############################################################################
    #    write PRIIPs paths into file                                             #
    ###############################################################################
    if len(list_FX_rates_toSimulate) != 0:
        script_dir = os.path.dirname(__file__)
        print('')
        print('[WRITING PATHS INTO FILE]')

        print('       (*) Favourable, Moderate, Unfavourable')
        for FX_rate, df_paths in dict_paths_FMU.items():
            filename = str_dateIdentifier + "-PATH_FMU_" + FX_rate + ".csv"
            df_paths.to_csv(os.path.join(script_dir + "/lib", filename), sep=',', header=False, index=False)

        print('       (*) Stressed')
        for FX_rate, df_paths in dict_paths_S.items():
            filename = str_dateIdentifier + "-PATH_S_" + FX_rate + ".csv"
            df_paths.to_csv(os.path.join(script_dir + "/lib", filename), sep=',', header=False, index=False)

    ###############################################################################
    #        PRIIPs PATHS - SIMULATE                                              #
    ###############################################################################
    for FX_rate in list_FX_rates_toReadPath:
       print('  (+) ReadingPaths ' + FX_rate)

       filename_FMU = str_dateIdentifier + "-PATH_FMU_" + FX_rate + ".csv"
       df_underlyer = read_PRIIPsPath(filename_FMU)
       dict_paths_FMU[FX_rate] = df_underlyer

       filename_S = str_dateIdentifier + "-PATH_S_" + FX_rate + ".csv"
       df_underlyer = read_PRIIPsPath(filename_S)
       dict_paths_S[FX_rate] = df_underlyer

    # add timer
    dict_timer['PRIIPsPaths'] = timeit.default_timer()

    ###############################################################################
    #        GrossAmounts                                                         #
    ###############################################################################
    print('')
    print('[CALCULATING GROSS AMOUNTS]')

    for product_id, product in enumerate(list_products):
        # skip products for which an error was raised
        if product.flag_error_encountered == True:
            continue

        list_requiredUnderlyers = product.get_requiredUnderlyers()

        dict_pathsFMU_forProduct = dict()
        dict_pathsS_forProduct   = dict()

        for key, item in dict_paths_FMU.items():
            if key in list_requiredUnderlyers:
                dict_pathsFMU_forProduct[key] = item
                dict_pathsS_forProduct[key]   = dict_paths_S[key]

        arr_payoffs_FMU = product.calculate_payoff(dict_paths = dict_pathsFMU_forProduct)
        arr_payoffs_S   = product.calculate_payoff(dict_paths = dict_pathsS_forProduct)

        product.update_payoff_FMU(arr_payoffs_FMU)
        product.update_payoff_S(arr_payoffs_S)

        product.calculate_GrossAmount_FMU()
        product.calculate_GrossAmount_S()

    # add timer
    dict_timer['GrossAmounts'] = timeit.default_timer()

    ###############################################################################
    #        NetAmounts, NetReturns, RIYs                                         #
    ###############################################################################
    print('')
    print('[CALCULATING NET AMOUNTS, NET RETURNS, RIYs]')

    for product_id, product in enumerate(list_products):
        # skip products for which an error was raised
        if product.flag_error_encountered == True:
            continue

        #-------------------------------------------------------------------------#
        #    Net Amounts                                                          #
        #-------------------------------------------------------------------------#
        product.calculate_NetAmounts()

        #-------------------------------------------------------------------------#
        #    Net Returns                                                          #
        #-------------------------------------------------------------------------#
        product.calculate_NetReturns()

        #-------------------------------------------------------------------------#
        #    RIYs                                                                 #
        #-------------------------------------------------------------------------#
        product.calculate_RIYs()

    # add timer
    dict_timer['NetNumbers'] = timeit.default_timer()

    ##########################################################################
    #        LOG ATTRIBUTES OF EACH PRODUCT                                  #
    ##########################################################################
    if dict_settings["writeProductLog"] == True:
        print('')
        print('[WRITING LOGFILE]')
        dict_LogInfo = dict()
        for product_id, product in enumerate(list_products):
            dict_attributes = getAllClassAttributes( class_instance = product )

            # check if we should drop payoff vectors
            #  --> they consume most of the storage space
            if dict_settings["writePayoffVector"] == False:
                # Favourable/Moderate/Unfavourable
                try:
                    del dict_attributes["arr_payoffs_FMU"]
                except KeyError:
                    pass

                # Stressed
                try:
                    del dict_attributes["arr_payoffs_S"]
                except KeyError:
                    pass

            dict_LogInfo[product_id] = dict_attributes
        script_dir = os.path.dirname(__file__)
        filename=dict_settings["writeProductLogFilename"]

        write_LogInfo_ClassAttributes(os.path.join(script_dir, filename), dict_LogInfo=dict_LogInfo)

    # add timer
    dict_timer['logFile'] = timeit.default_timer()

    ##########################################################################
    #        PRODUCE REO.csv                                                 #
    ##########################################################################
    print('')
    print('[WRITING REO.csv]')

    df_REO = pd.DataFrame()

    for product_id, product in enumerate(list_products):
        # skip products for which an error was raised
        if product.flag_error_encountered == True:
            continue

        dict_new_row = product.produce_REO_output()
        df_new_row   = pd.DataFrame(dict_new_row, index=[product_id])
        df_REO = df_REO.append(df_new_row)
    script_dir = os.path.dirname(__file__)
    REO_columnOrder = get_REO_columnOrder()
    df_REO.to_csv(os.path.join(script_dir, 'MyREO.csv'), sep=',', header=True, index=False, columns=REO_columnOrder)

    # add timer
    dict_timer['writeREO'] = timeit.default_timer()

    ###############################################################################
    #        PRODUCE ATTRIBUTES                                                   #
    ###############################################################################
    print('')
    print('[WRITING Attributes.txt]')

    list_Attributes = []
    for product_id, product in enumerate(list_products):
        # skip products for which an error was raised
        if product.flag_error_encountered == True:
            continue

        dict_NewAttributes = product.produce_Attributes_output()
        list_Attributes.append(dict_NewAttributes)
    os.path.join(script_dir, 'MyREO.csv')
    write_AttributesFile(list_Attributes = list_Attributes, filename=os.path.join(script_dir, "MyAttributes.txt"))

    # add timer
    dict_timer['writeAttributes'] = timeit.default_timer()

    ###############################################################################
    #        MEASURE ELAPSED TIME                                                 #
    ###############################################################################
    dict_timer['end'] = timeit.default_timer()

    total_time = dict_timer['end']-dict_timer['start']

    print('')
    print('[RUNTIME]. Total: ', round(dict_timer['end']-dict_timer['start'], 3), 'seconds')
    print('            (+) Read Configuration Files   : %06.3f seconds [%04.1f %%]' % (dict_timer['detectUnderlyings']        - dict_timer['start']                   , (dict_timer['detectUnderlyings']        - dict_timer['start'])/total_time*100))
    print('            (+) Read Current MarketData    : %06.3f seconds [%04.1f %%]' % (dict_timer['readCurrentMarketData']    - dict_timer['detectUnderlyings']       , (dict_timer['readCurrentMarketData']    - dict_timer['detectUnderlyings'])/total_time*100))
    print('            (+) Read Historical MarketData : %06.3f seconds [%04.1f %%]' % (dict_timer['readHistoricalMarketData'] - dict_timer['readCurrentMarketData']   , (dict_timer['readHistoricalMarketData'] - dict_timer['readCurrentMarketData'])/total_time*100))
    print('            (+) Read Random Indices        : %06.3f seconds [%04.1f %%]' % (dict_timer['readRandomIndices']        - dict_timer['readHistoricalMarketData'], (dict_timer['readRandomIndices']        - dict_timer['readHistoricalMarketData'])/total_time*100))
    print('            (+) Pre-Processing             : %06.3f seconds [%04.1f %%]' % (dict_timer['preProcessing']            - dict_timer['readMappings']            , (dict_timer['preProcessing']            - dict_timer['readMappings'])/total_time*100))
    print('            (+) Get PRIIPs paths           : %06.3f seconds [%04.1f %%]' % (dict_timer['PRIIPsPaths']              - dict_timer['BookKeeping']             , (dict_timer['PRIIPsPaths']              - dict_timer['BookKeeping'])/total_time*100))
    print('            (+) Compute Gross Amounts      : %06.3f seconds [%04.1f %%]' % (dict_timer['GrossAmounts']             - dict_timer['PRIIPsPaths']             , (dict_timer['GrossAmounts']             - dict_timer['PRIIPsPaths'])/total_time*100))
    print('            (+) Compute Net numbers        : %06.3f seconds [%04.1f %%]' % (dict_timer['NetNumbers']               - dict_timer['GrossAmounts']            , (dict_timer['NetNumbers']               - dict_timer['GrossAmounts'])/total_time*100))
    print('            (+) Write LogFile              : %06.3f seconds [%04.1f %%]' % (dict_timer['logFile']                  - dict_timer['NetNumbers']              , (dict_timer['logFile']                  - dict_timer['NetNumbers'])/total_time*100))
    print('            (+) Write RiskEngineOutput     : %06.3f seconds [%04.1f %%]' % (dict_timer['writeREO']                 - dict_timer['logFile']                 , (dict_timer['writeREO']                 - dict_timer['logFile'])/total_time*100))
    print('            (+) Write Attributes           : %06.3f seconds [%04.1f %%]' % (dict_timer['writeAttributes']          - dict_timer['writeREO']                , (dict_timer['writeAttributes']          - dict_timer['writeREO'])/total_time*100))


    return(df_REO)
