import numpy as np
from .PreProcessing import calculate_strike, \
                              get_SpotRate    , \
                              get_Yield
from .DateLogic import getYearFraction                , \
                          addTenor                       , \
                          networkdays                    , \
                          getHolidaysBetween             , \
                          convertDateFormatForAttributes , \
                          getTenorInformation

class class_FX_Forward:
    def __init__(self,
                 ccy_FOR         ,
                 ccy_DOM         ,
                 ccy_SET         ,
                 ReceiveCurrency ,
                 ReceiveAmount   ,
                 PayCurrency     ,
                 PayAmount       ,
                 RHP_string      ,
                 positionType    ,
                 cost_input_perc ,
                 deliveryType    ,
                 MRM             ,
                 SRI             ,
                 CRM             ):

        self.ccy_FOR         = ccy_FOR
        self.ccy_DOM         = ccy_DOM
        self.ccy_SET         = ccy_SET
        self.ReceiveCurrency = ReceiveCurrency
        self.ReceiveAmount   = ReceiveAmount
        self.PayCurrency     = PayCurrency
        self.PayAmount       = PayAmount
        self.RHP_string      = RHP_string
        self.positionType    = positionType
        self.cost_input_perc = cost_input_perc
        self.deliveryType    = deliveryType
        self.MRM             = MRM
        self.SRI             = SRI
        self.CRM             = CRM

        self.product_type    = 'FX_Forward'
        self.product_id      = None

        self.arr_payoffs_FMU = np.array([]) # payoffs from Favourable, Moderate and Unfavourable Scenario
        self.arr_payoffs_S   = np.array([]) # payoffs from Stressed Scenario

        self.GrossAmount_Favourable   = None
        self.GrossAmount_Moderate     = None
        self.GrossAmount_Unfavourable = None
        self.GrossAmount_Stressed     = None

        # if at any point an error is encountered: skip this product for everything that follows
        self.flag_error_encountered = False

    ###########################################################################
    #              SET THE PRODUCT ID                                         #
    ###########################################################################
    def set_productID(self, product_id):
        self.product_id = product_id

    ###########################################################################
    #              SET THE TRADE DATE                                         #
    ###########################################################################
    def set_tradeDate(self, str_date):
        self.tradeDate = str_date

    ###########################################################################
    #              PRE-PROCESSING                                             #
    ###########################################################################
    def preProcessing(self,
                      df_CurrentMarketData_Exchange,
                      df_CurrentMarketData_Yield,
                      dict_YieldCcy_CurveName):
        #.....................................................................#
        #          calculate spot/fixing/settlement dates                     #
        #.....................................................................#
        self.spotDate       = addTenor(self.tradeDate, '2D')
        self.settlementDate = addTenor(self.spotDate , self.RHP_string)

        # for deliverable Forward: there is no fixingDate --> set it to same value as settlementDate
        if self.deliveryType == 'deliverable':
            self.fixingDate = self.settlementDate

        # for NDF: fixingDate two days before settlementDate
        elif self.deliveryType == 'nondeliverable':
            self.fixingDate = addTenor(self.settlementDate, '-2D')

        else:
            print('[ERROR]. DeliveryType not recognized. ', deliveryType)
            print('         Needs to be deliverable or nondeliverable')
            self.flag_error_encountered = True
            return -1

        #.....................................................................#
        #          calculate RHP YearFraction                                 #
        #.....................................................................#
        self.T_RHP = getYearFraction( start_date = self.tradeDate       ,
                                      end_date   = self.fixingDate      ,
                                      BusinessDayConvention = 'ACTACT'  )

        #.....................................................................#
        #          calculate nTradingDaysRHP                                  #
        #.....................................................................#
        holiday_list = getHolidaysBetween( self.tradeDate               ,
                                           self.fixingDate              ,
                                           COUNTRY_HOLIDAYS = 'England' )


        self.nTradingDaysRHP = networkdays( self.tradeDate      ,
                                            self.fixingDate     ,
                                            holiday_list        )

        #.....................................................................#
        #          determine required underlyers                              #
        #.....................................................................#
        # just one underlyer for FX_Forward
        self.list_requiredUnderlyers = [self.ccy_FOR+self.ccy_DOM]

        #.....................................................................#
        #          calculate strike                                           #
        #.....................................................................#
        spot_rate = get_SpotRate(df_CurrentMarketData_Exchange = df_CurrentMarketData_Exchange, \
                                 ccy_FOR                       = self.ccy_FOR              , \
                                 ccy_DOM                       = self.ccy_DOM              )
        self.spot_rate = spot_rate
        # check if data was found
        if spot_rate == None:
            print('[ERROR - Forward]. Could not find spot_rate.')
            print('               ', ccy_FOR, ccy_DOM)
            print('               This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1

        tenor_in_days = int(self.T_RHP * 360)

        yield_FOR = get_Yield( df_CurrentMarketData_Yield = df_CurrentMarketData_Yield , \
                               dict_YieldCcy_CurveName    = dict_YieldCcy_CurveName    , \
                               tenor_in_days              = tenor_in_days              , \
                               ccy_pair                   = self.ccy_FOR+self.ccy_DOM  , \
                               ccy_name                   = self.ccy_FOR               )
        self.yield_FOR = yield_FOR

        yield_DOM = get_Yield( df_CurrentMarketData_Yield = df_CurrentMarketData_Yield , \
                               dict_YieldCcy_CurveName    = dict_YieldCcy_CurveName    , \
                               tenor_in_days              = tenor_in_days              , \
                               ccy_pair                   = self.ccy_FOR+self.ccy_DOM  , \
                               ccy_name                   = self.ccy_DOM               )
        self.yield_DOM = yield_DOM

        self.strike = calculate_strike(yield_FOR            = self.yield_FOR  , \
                                       yield_DOM            = self.yield_DOM  , \
                                       spot_rate            = self.spot_rate  , \
                                       T                    = self.T_RHP      , \
                                       n_compoundingPeriods = 1)

        #.....................................................................#
        #          calculate PayAmount & ReceiveAmount                        #
        #.....................................................................#
        # sanity check: currency denomination Pay/Receive currency
        if self.ReceiveCurrency != self.ccy_FOR:
            print('[ERROR]. ReceiveCurrency is not the same as FOR currency.')
            print('         This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1

        if self.PayCurrency != self.ccy_DOM:
            print('[ERROR]. PayCurrency is not the same as DOM currency.')
            print('         This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1

        # EURUSD, EURGBP, GBPUSD, USDINR
        if self.PayAmount == None and self.ReceiveAmount == 10000:
            PayAmount = self.ReceiveAmount * self.strike
            self.PayAmount = PayAmount

        # XAUUSD
        elif self.ReceiveAmount == None and self.PayAmount == 10000:
            ReceiveAmount = self.PayAmount / self.strike
            self.ReceiveAmount = ReceiveAmount

        # ERROR
        else:
            print('[ERROR]. PayAmount / ReceiveAmount not set correctly.')
            print('         Should be 10,000 and None, but was:')
            print('           ReceiveAmount: ' + str(self.ReceiveAmount))
            print('           PayAmount:     ' + str(self.PayAmount))

            print('         This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1



        #.....................................................................#
        #          if no error encountered: return 0                          #
        #.....................................................................#
        return 0

    ###########################################################################
    #              SET ERROR MESSAGE                                          #
    ###########################################################################
    def set_ErrorMessage(self):
        """
        If the flag flag_error_encountered is set to True, an error occurred
        at some point for the product. Skip it for all further steps.
        """
        self.flag_error_encountered = True

    ###########################################################################
    #              RETURN LIST WITH REQUIRED UNDERLYERS                       #
    ###########################################################################
    def get_requiredUnderlyers(self):
        return self.list_requiredUnderlyers

    ###########################################################################
    #              SET THE FAV / MOD / UNFAV PAYOFFS                          #
    ###########################################################################
    def update_payoff_FMU(self, arr_payoff):
        self.arr_payoffs_FMU = arr_payoff

    ###########################################################################
    #              SET THE STRESSED PAYOFFS                                   #
    ###########################################################################
    def update_payoff_S(self, arr_payoff):
        self.arr_payoffs_S = arr_payoff

    ###########################################################################
    #              GROSS AMOUNTS FAV / MOD / UNFAV                            #
    ###########################################################################
    def calculate_GrossAmount_FMU(self):
        self.GrossAmount_Favourable   = np.percentile(self.arr_payoffs_FMU, 90.0)
        self.GrossAmount_Moderate     = np.percentile(self.arr_payoffs_FMU, 50.0)
        self.GrossAmount_Unfavourable = np.percentile(self.arr_payoffs_FMU, 10.0)

    ###########################################################################
    #              GROSS AMOUNTS STRESSED                                     #
    ###########################################################################
    def calculate_GrossAmount_S(self):
        # this is only true for products with tenor <= 1Y
        self.GrossAmount_Stressed = np.percentile(self.arr_payoffs_S, 1.0)

    ###########################################################################
    #              NET AMOUNTS                                                #
    ###########################################################################
    def calculate_NetAmounts(self):
        self.ScalingFactor    = 1.0 / (1.0 + self.cost_input_perc)
        self.Cost             = 10000.0 * self.ScalingFactor * self.cost_input_perc
        self.NotionalInvested = 10000.0 * self.ScalingFactor

        self.calculate_fFX()

        self.NetAmount_Favourable   = self.fFX_Favourable   * self.NotionalInvested - 10000.0
        self.NetAmount_Moderate     = self.fFX_Moderate     * self.NotionalInvested - 10000.0
        self.NetAmount_Unfavourable = self.fFX_Unfavourable * self.NotionalInvested - 10000.0
        self.NetAmount_Stressed     = self.fFX_Stressed     * self.NotionalInvested - 10000.0

    ###########################################################################
    #              NET RETURNS                                                #
    ###########################################################################
    def calculate_NetReturns(self):
        self.NetReturn_Favourable   = self.fFX_Favourable   / (1.0 + self.cost_input_perc) - 1.0
        self.NetReturn_Moderate     = self.fFX_Moderate     / (1.0 + self.cost_input_perc) - 1.0
        self.NetReturn_Unfavourable = self.fFX_Unfavourable / (1.0 + self.cost_input_perc) - 1.0
        self.NetReturn_Stressed     = self.fFX_Stressed     / (1.0 + self.cost_input_perc) - 1.0

    ###########################################################################
    #              RIY                                                        #
    ###########################################################################
    def calculate_RIYs(self):
        self.RIYRHP = np.power((self.fFX_Moderate/1.0), (1.0/self.T_RHP)) - \
                      np.power((self.fFX_Moderate/(1.0+self.cost_input_perc)), (1.0/self.T_RHP))


    ###########################################################################
    #              f(FX)                                                      #
    ###########################################################################
    def calculate_fFX(self):
        self.fFX_Favourable   = self.GrossAmount_Favourable  / 10000.0 + 1
        self.fFX_Moderate     = self.GrossAmount_Moderate    / 10000.0 + 1
        self.fFX_Unfavourable = self.GrossAmount_Unfavourable/ 10000.0 + 1
        self.fFX_Stressed     = self.GrossAmount_Stressed    / 10000.0 + 1


    ###########################################################################
    #              COMPUTE PAYOFF GIVEN PATHS                                 #
    ###########################################################################
    def calculate_payoff(self, dict_paths):

        for underlyer in self.list_requiredUnderlyers:
            df_path = dict_paths[underlyer]

            # pick the underlying timepoint for payoff evaluation
            arr_underlying = df_path.iloc[self.nTradingDaysRHP-1]

            # EURUSD, EURGBP, USDINR, ...
            if self.ccy_SET == self.ccy_FOR:
                if self.positionType == 'long':
                    arr_payoff = self.ReceiveAmount / arr_underlying * (arr_underlying - self.strike)
                elif self.positionType == 'short':
                    arr_payoff = (-1)*self.ReceiveAmount / arr_underlying * (arr_underlying - self.strike)
                else:
                    arr_payoff = np.NaN

            # XAUUSD
            elif self.ccy_SET == self.ccy_DOM:
                if self.positionType == 'long':
                    arr_payoff = self.ReceiveAmount * (arr_underlying - self.strike)
                elif self.positionType == 'short':
                    arr_payoff = (-1)*self.ReceiveAmount * (arr_underlying - self.strike)
                else:
                    arr_payoff = np.NaN

            return arr_payoff

    ###########################################################################
    #              PRODUCE RiskEngineOutput.csv                               #
    ###########################################################################
    def produce_REO_output(self):

        line_start = "FX-Forward-"
        if self.deliveryType == "deliverable":
            line_start += "Deliverable-"
        else:
            line_start +=  "Non-Deliverable-"
        line_start += self.ccy_FOR + "-" + self.ccy_DOM + "-"
        if self.positionType == "long":
            line_start += "Long-"
        else:
            line_start +=  "Short-"
        line_start += str(self.product_id)

        new_row = { \
                    'id'                : line_start,
                     'MRM'              : self.MRM,
                     'CRM'              : self.CRM,
                     'VAR'              : 0,
                     'VEV'              : 0,
                     'HistoricalVEV'    : 0,
                     'SRI'              : self.SRI,
                     'InvestmentAmount' : 10000.0,
                     'InvestmentCurrency': self.ccy_SET,
                     'EquivalentAmount' : 10000.0,
                     'ScalingFactor' : self.ScalingFactor,
                     'Pricing_Method' : 'Bootstrap VAR',
                     'CarriedInterest_IHP1' : '',
                     'CarriedInterest_IHP2' : '',
                     'CarriedInterest_RHP' : '',
                     'EntryCostAcquired_IHP1' : '',
                     'EntryCostAcquired_IHP2' : '',
                     'EntryCostAcquired_RHP' : '',
                     'Entry_IHP1' : '',
                     'Entry_IHP2' : '',
                     'Entry_RHP' : '',
                     'ExitCostAt1Year_IHP1' : '',
                     'ExitCostAt1Year_IHP2' : '',
                     'ExitCostAt1Year_RHP' : '',
                     'ExitCostAtHalfRHP_IHP1' : '',
                     'ExitCostAtHalfRHP_IHP2' : '',
                     'ExitCostAtHalfRHP_RHP' : '',
                     'ExitCostAtRHP_IHP1' : '',
                     'ExitCostAtRHP_IHP2' : '',
                     'ExitCostAtRHP_RHP' : '',
                     'Exit_IHP1' : '',
                     'Exit_IHP2' : '',
                     'Exit_RHP' : '',
                     'Other_IHP1' : '',
                     'Other_IHP2' : '',
                     'Other_RHP' : '',
                     'PerformanceFees_IHP1' : '',
                     'PerformanceFees_IHP2' : '',
                     'PerformanceFees_RHP' : '',
                     'Transaction_IHP1' : '',
                     'Transaction_IHP2' : '',
                     'Transaction_RHP' : '',
                     'TotalCostsIHP1' : '',
                     'TotalCostsIHP2' : '',
                     'TotalCostsRHP': self.Cost,
                     'IHP1Date' : '',
                     'IHP1Offset' : '',
                     'IHP2Date' : '',
                     'IHP2Offset' : '',
                     'RHPDate' : '',
                     'RHPOffset' : self.T_RHP,
                     'RiskEngineOuput_PSFavourable_ScenarioIHP1' : '',
                     'RiskEngineOuput_PSFavourable_ScenarioIHP2' : '',
                     'RiskEngineOuput_PSFavourable_ScenarioRHP': self.GrossAmount_Favourable,
                     'RiskEngineOuput_PSModerate_ScenarioIHP1' : '',
                     'RiskEngineOuput_PSModerate_ScenarioIHP2' : '',
                     'RiskEngineOuput_PSModerate_ScenarioRHP': self.GrossAmount_Moderate,
                     'RiskEngineOuput_PSUnfavourable_ScenarioIHP1' : '',
                     'RiskEngineOuput_PSUnfavourable_ScenarioIHP2' : '',
                     'RiskEngineOuput_PSUnfavourable_ScenarioRHP': self.GrossAmount_Unfavourable,
                     'RiskEngineOuput_PSStressed_ScenarioIHP1' : '',
                     'RiskEngineOuput_PSStressed_ScenarioIHP2' : '',
                     'RiskEngineOuput_PSStressed_ScenarioRHP': self.GrossAmount_Stressed,
                     'NetPerformanceAmount_Favourable_ScenarioIHP1' : '',
                     'NetPerformanceAmount_Favourable_ScenarioIHP2' : '',
                     'NetPerformanceAmount_Favourable_ScenarioRHP' : self.NetAmount_Favourable,
                     'NetPerformanceAmount_Moderate_ScenarioIHP1' : '',
                     'NetPerformanceAmount_Moderate_ScenarioIHP2' : '',
                     'NetPerformanceAmount_Moderate_ScenarioRHP' : self.NetAmount_Moderate,
                     'NetPerformanceAmount_Unfavourable_ScenarioIHP1' : '',
                     'NetPerformanceAmount_Unfavourable_ScenarioIHP2' : '',
                     'NetPerformanceAmount_Unfavourable_ScenarioRHP' : self.NetAmount_Unfavourable,
                     'NetPerformanceAmount_Stressed_ScenarioIHP1' : '',
                     'NetPerformanceAmount_Stressed_ScenarioIHP2' : '',
                     'NetPerformanceAmount_Stressed_ScenarioRHP' : self.NetAmount_Stressed,
                     'NetPerformanceReturn_Favourable_ScenarioIHP1' : '',
                     'NetPerformanceReturn_Favourable_ScenarioIHP2' : '',
                     'NetPerformanceReturn_Favourable_ScenarioRHP': self.NetReturn_Favourable,
                     'NetPerformanceReturn_Moderate_ScenarioIHP1' : '',
                     'NetPerformanceReturn_Moderate_ScenarioIHP2' : '',
                     'NetPerformanceReturn_Moderate_ScenarioRHP': self.NetReturn_Moderate,
                     'NetPerformanceReturn_Unfavourable_ScenarioIHP1' : '',
                     'NetPerformanceReturn_Unfavourable_ScenarioIHP2' : '',
                     'NetPerformanceReturn_Unfavourable_ScenarioRHP': self.NetReturn_Unfavourable,
                     'NetPerformanceReturn_Stressed_ScenarioIHP1' : '',
                     'NetPerformanceReturn_Stressed_ScenarioIHP2' : '',
                     'NetPerformanceReturn_Stressed_ScenarioRHP': self.NetReturn_Stressed,
                     'RIYCarriedInterest' : '',
                     'RIYEntryCostAcquired' : '',
                     'RIYEntry': self.RIYRHP,
                     'RIYExitCostAt1Year' : '',
                     'RIYExitCostAtHalfRHP' : '',
                     'RIYExitCostAtRHP' : '',
                     'RIYExit' : '',
                     'RIYOther' : '',
                     'RIYPerformanceFees' : '',
                     'RIYTransaction' : '',
                     'RIYIHP1' : '',
                     'RIYIHP2' : '',
                     'RIYRHP' : self.RIYRHP,
                     'CF_Moment_M0' : '',
                     'CF_Moment_M1' : '',
                     'CF_Moment_M2' : '',
                     'CF_Moment_M3' : '',
                     'CF_Moment_M4' : '',
                     'CF_Stats_Skew ' : '',
                     'CF_Stats_Volatility' : '',
                     'CF_Stats_Kurtosis' : '',
                     'Stressed_Volatility_IHP1' : '',
                     'Stressed_Volatility_IHP2' : '',
                     'Stressed_Volatility_RHP' : '',
                     'Stressed_Z_Alpha_IHP1' : '',
                     'Stressed_Z_Alpha_IHP2' : '',
                     'Stressed_Z_Alpha_RHP' : '',
                 }

        return new_row

    ###########################################################################
    #              PRODUCE Attributes.txt                                     #
    ###########################################################################
    def produce_Attributes_output(self):
        """
        create all lines that are required for Attributes.txt
        This is product specific information.

        Output:
        a list of lines that need to be written into Attributes.txt
        """

        #---------------------------------------------------------------------#
        # construct String at beginning of each line                          #
        #---------------------------------------------------------------------#
        line_start = "FX-Forward-"
        if self.deliveryType == "deliverable":
            line_start += "Deliverable-"
        else:
            line_start +=  "Non-Deliverable-"
        line_start += self.ccy_FOR + "-" + self.ccy_DOM + "-"
        if self.positionType == "long":
            line_start += "Long-"
        else:
            line_start +=  "Short-"
        line_start += str(self.product_id)

        #---------------------------------------------------------------------#
        # collect all information needed in dictionary                        #
        #---------------------------------------------------------------------#
        dict_attributes = dict()

        dict_attributes["LineStart"]    = line_start

        dict_attributes["BuyAmount"]    = str(self.ReceiveAmount)

        dict_attributes["BuyCurrency"]  = str(self.ReceiveCurrency)

        dict_attributes["FXRate"]       = str(self.spot_rate)

        dict_attributes["Illustrative"] = "1"

        if self.ccy_FOR == "XAU":
            dict_attributes["IsReportingCurrencyBullion"] = "TRUE"
        else:
            dict_attributes["IsReportingCurrencyBullion"] = "FALSE"

        if self.ccy_DOM == "XAU":
            dict_attributes["IsSecondaryCurrencyBullion"] = "TRUE"
        else:
            dict_attributes["IsSecondaryCurrencyBullion"] = "FALSE"

        dict_attributes["NotionalAmount"] = str(self.ReceiveAmount)

        # TODO - sort this one out
        dict_attributes["PremiumPaymentDate"] = convertDateFormatForAttributes(self.tradeDate)
        if dict_attributes["PremiumPaymentDate"] == None:
            self.flag_error_encountered = True
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')

        if self.deliveryType == "deliverable":
            dict_attributes["ProductName"] = "Over the Counter Deliverable FX Forward"
        elif self.deliveryType == "nondeliverable":
            dict_attributes["ProductName"] = "Over the Counter Non-Deliverable FX Forward"
        else:
            print('[ERROR - produce_Attributes_output]. ')
            print('         DeliveryType can only be deliverable or nondeliverable')
            print('         Found: ', self.deliveryType)
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')
            self.flag_error_encountered = True

        dict_attributes["ReportingCurrency"] = str(self.ccy_SET)

        dict_attributes["SecondCurrency"] = str(self.ccy_DOM)

        dict_attributes["SellAmount"] = str(self.PayAmount)

        dict_attributes["SellCurrency"] = str(self.PayCurrency)

        dict_attributes["SettlementDate"] = convertDateFormatForAttributes(self.settlementDate)
        if dict_attributes["SettlementDate"] == None:
            self.flag_error_encountered = True
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')

        # TODO - sort this one out
        dict_attributes["SettlementExpiration"] = "settlement"

        subtype = "FX-Forward-"
        if self.deliveryType == "deliverable":
            line_start += "Deliverable-"
        else:
            line_start +=  "Non-Deliverable-"
        if self.positionType == "long":
            line_start += "Long"
        else:
            line_start +=  "Short"
        dict_attributes["Subtype"] = subtype

        # TODO - sort this one out
        dict_attributes["TableDate"] = convertDateFormatForAttributes(self.settlementDate)
        if dict_attributes["TableDate"] == None:
            self.flag_error_encountered = True
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')

        tenorMultiplier, tenorPeriod = getTenorInformation(self.RHP_string)
        if tenorMultiplier == None:
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')
            self.flag_error_encountered = True
        dict_attributes["TenorMultiplier"] = str(tenorMultiplier)
        dict_attributes["TenorPeriod"]     = tenorPeriod

        dict_attributes["TradeDate"] = convertDateFormatForAttributes(self.tradeDate)
        if dict_attributes["TradeDate"] == None:
            self.flag_error_encountered = True
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')

        return dict_attributes
