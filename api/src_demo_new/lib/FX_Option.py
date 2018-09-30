import numpy as np
import scipy.stats as ss
from .PreProcessing import calculate_strike, \
                              get_SpotRate    , \
                              get_Yield       , \
                              get_FXATMVol
from .DateLogic import getYearFraction                , \
                          addTenor                       , \
                          networkdays                    , \
                          getHolidaysBetween             , \
                          convertDateFormatForAttributes , \
                          getTenorInformation

from .Pricing import value_FX_Option_GarmanKohlhagen


class class_FX_Option:
    def __init__(self,
                 ccy_FOR         ,
                 ccy_DOM         ,
                 ccy_SET         ,
                 CallCurrency    ,
                 CallAmount      ,
                 PutCurrency     ,
                 PutAmount       ,
                 RHP_string      ,
                 positionType    ,
                 optionType      ,
                 cost_input_perc ,
                 deliveryType    ,
                 MRM             ,
                 SRI             ,
                 CRM             ):

        self.ccy_FOR         = ccy_FOR
        self.ccy_DOM         = ccy_DOM
        self.ccy_SET         = ccy_SET
        self.CallCurrency    = CallCurrency
        self.CallAmount      = CallAmount
        self.PutCurrency     = PutCurrency
        self.PutAmount       = PutAmount
        self.RHP_string      = RHP_string
        self.positionType    = positionType
        self.optionType      = optionType
        self.cost_input_perc = cost_input_perc
        self.deliveryType    = deliveryType
        self.MRM             = MRM
        self.SRI             = SRI
        self.CRM             = CRM

        self.product_type    = 'FX_Option'
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
                      df_CurrentMarketData_Exchange ,
                      df_CurrentMarketData_Yield    ,
                      dict_YieldCcy_CurveName       ,
                      df_CurrentMarketData_FXATMVol ,
                      dict_FXATMVol_CurveName       ):

        #.....................................................................#
        #          calculate spot/fixing/settlement dates                     #
        #.....................................................................#
        self.spotDate       = addTenor(self.tradeDate, '2D')
        self.settlementDate = addTenor(self.spotDate , self.RHP_string)
        self.fixingDate     = addTenor(self.settlementDate, '-2D')

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
            print('[ERROR - Option]. Could not find spot_rate.')
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
        #          calculate CallAmount & PutAmount                           #
        #.....................................................................#
        # sanity check: currency denomination Call/Put currency
        if self.CallCurrency != self.ccy_FOR:
            print('[ERROR]. CallCurrency is not the same as FOR currency.')
            print('         This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1
        if self.PutCurrency != self.ccy_DOM:
            print('[ERROR]. PutCurrency is not the same as DOM currency.')
            print('         This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1

        # EURUSD, EURGBP, GBPUSD, USDINR
        if self.PutAmount == None and self.CallAmount == 10000:
            PutAmount = self.CallAmount * self.strike
            self.PutAmount = PutAmount

        # XAUUSD
        elif self.CallAmount == None and self.PutAmount == 10000:
            CallAmount = self.PutAmount / self.strike
            self.CallAmount  = CallAmount

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
        #          set the DiscountYield (for Short Options -> NetCalcs)      #
        #.....................................................................#
        if self.positionType == 'short':
            # EURUSD, GBPUSD, ...
            if self.ccy_SET == self.ccy_FOR:
                self.DiscountRate = yield_FOR

            # XAUUSD
            elif self.ccy_SET == self.ccy_DOM:
                self.DiscountRate = yield_DOM

            else:
                print('[ERROR]. DiscountRate could not be set. FOR, DOM and SET currency issues...')
                self.flag_error_encountered = True
                return -1

        #.....................................................................#
        #          value the option                                           #
        #.....................................................................#
        # the vol surface for XAUUSD is messed up (comes as USDXAU instead of XAUUSD)
        if self.ccy_FOR == 'XAU':
            ccy_name_counter = 'XAU'
        else:
            ccy_name_counter = self.ccy_DOM

        volatility = get_FXATMVol( df_CurrentMarketData_FXATMVol = df_CurrentMarketData_FXATMVol , \
                                   dict_FXATMVol_CurveName       = dict_FXATMVol_CurveName       , \
                                   tenor_in_days                 = tenor_in_days                 , \
                                   ccy_name                      = self.ccy_FOR                  , \
                                   ccy_name_counter              = ccy_name_counter              )
        self.volatility_ATM = volatility

        OptionValue = value_FX_Option_GarmanKohlhagen( spot       = spot_rate       , \
                                                       strike     = self.strike     , \
                                                       volatility = volatility      , \
                                                       yield_FOR  = yield_FOR       , \
                                                       yield_DOM  = yield_DOM       , \
                                                       T          = self.T_RHP      , \
                                                       ccy_FOR    = self.ccy_FOR    , \
                                                       ccy_DOM    = self.ccy_DOM    , \
                                                       ccy_SET    = self.ccy_SET    , \
                                                       optionType = self.optionType , \
                                                       CallAmount = self.CallAmount )

        if OptionValue == None:
            self.flag_error_encountered = True
            return -1
        self.OptionValue = OptionValue

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
        #--------------#
        # Long Option  #
        #--------------#
        if self.positionType == 'long':
            self.ScalingFactor    = 1.0 / (self.OptionValue/10000.0 + self.cost_input_perc)
            self.Cost             = 10000.0 * self.ScalingFactor * self.cost_input_perc
            self.NotionalInvested = 10000.0 * self.ScalingFactor * self.OptionValue/10000.0
            self.TotalNotional    = 10000.0 / (self.OptionValue/10000.0 + self.cost_input_perc)

            self.calculate_fFX()

            self.NetAmount_Favourable   = self.fFX_Favourable   * self.TotalNotional
            self.NetAmount_Moderate     = self.fFX_Moderate     * self.TotalNotional
            self.NetAmount_Unfavourable = self.fFX_Unfavourable * self.TotalNotional
            self.NetAmount_Stressed     = self.fFX_Stressed     * self.TotalNotional

        #--------------#
        # Short Option #
        #--------------#
        elif self.positionType == 'short':
            self.ScalingFactor    = 1.0 / (1.0 + self.cost_input_perc)
            self.Cost             = 10000.0 * self.ScalingFactor * self.cost_input_perc
            self.NotionalInvested = 10000.0 * self.ScalingFactor

            self.calculate_fFX()

            self.NetAmount_Favourable   = 10000.0 * \
                                          ( (self.OptionValue/10000.0 - self.cost_input_perc)        * \
                                            np.exp(self.DiscountRate*self.T_RHP) + self.fFX_Favourable ) / \
                                          (1.0 + self.cost_input_perc)
            self.NetAmount_Moderate     = 10000.0 * \
                                          ( (self.OptionValue/10000.0 - self.cost_input_perc)        * \
                                            np.exp(self.DiscountRate*self.T_RHP) + self.fFX_Moderate   ) / \
                                          (1.0 + self.cost_input_perc)
            self.NetAmount_Unfavourable = 10000.0 * \
                                          ( (self.OptionValue/10000.0 - self.cost_input_perc)          * \
                                            np.exp(self.DiscountRate*self.T_RHP) + self.fFX_Unfavourable ) / \
                                          (1.0 + self.cost_input_perc)
            self.NetAmount_Stressed     = 10000.0 * \
                                          ( (self.OptionValue/10000.0 - self.cost_input_perc)        * \
                                            np.exp(self.DiscountRate*self.T_RHP) + self.fFX_Stressed   ) / \
                                          (1.0 + self.cost_input_perc)
        else:
            print('[ERROR]. Option NetAmount Calculation.')
            print('         positionType can only be short or long.')

    ###########################################################################
    #              NET RETURNS                                                #
    ###########################################################################
    def calculate_NetReturns(self):
        #--------------#
        # Long Option  #
        #--------------#
        if self.positionType == 'long':
            self.NetReturn_Favourable   = self.fFX_Favourable   / (self.OptionValue/ 10000.0 + self.cost_input_perc) - 1.0
            self.NetReturn_Moderate     = self.fFX_Moderate     / (self.OptionValue/ 10000.0 + self.cost_input_perc) - 1.0
            self.NetReturn_Unfavourable = self.fFX_Unfavourable / (self.OptionValue/ 10000.0 + self.cost_input_perc) - 1.0
            self.NetReturn_Stressed     = self.fFX_Stressed     / (self.OptionValue/ 10000.0 + self.cost_input_perc) - 1.0

        #--------------#
        # Short Option #
        #--------------#
        elif self.positionType == 'short':
            self.NetReturn_Favourable   = self.NetAmount_Favourable   / 10000.0
            self.NetReturn_Moderate     = self.NetAmount_Moderate     / 10000.0
            self.NetReturn_Unfavourable = self.NetAmount_Unfavourable / 10000.0
            self.NetReturn_Stressed     = self.NetAmount_Stressed     / 10000.0

        else:
            print('[ERROR]. Option NetReturn Calculation.')
            print('         positionType can only be short or long.')

    ###########################################################################
    #              RIY                                                        #
    ###########################################################################
    def calculate_RIYs(self):
        #--------------#
        # Long Option  #
        #--------------#
        if self.positionType == 'long':
            i = np.power((self.fFX_Moderate/(self.OptionValue/10000.0)), (1.0/self.T_RHP))
            r = np.power((self.fFX_Moderate/((self.OptionValue/10000.0)+self.cost_input_perc)), \
                         (1.0/self.T_RHP))

            self.RIYRHP = i - r

        #--------------#
        # Short Option #
        #--------------#
        elif self.positionType == 'short':
            i = np.power( ( (self.OptionValue/10000.0) * \
                           np.exp(self.DiscountRate*self.T_RHP) + self.fFX_Moderate), (1.0/self.T_RHP) )
            r = np.power( ( (self.OptionValue/10000.0 - self.cost_input_perc) * \
                           np.exp(self.DiscountRate*self.T_RHP) + self.fFX_Moderate), (1.0/self.T_RHP) )
            self.RIYRHP = i - r

        else:
            print('[ERROR]. Option RIY Calculation.')
            print('         positionType can only be short or long.')

    ###########################################################################
    #              f(FX)                                                      #
    ###########################################################################
    def calculate_fFX(self):
        self.fFX_Favourable   = self.GrossAmount_Favourable  / 10000.0
        self.fFX_Moderate     = self.GrossAmount_Moderate    / 10000.0
        self.fFX_Unfavourable = self.GrossAmount_Unfavourable/ 10000.0
        self.fFX_Stressed     = self.GrossAmount_Stressed    / 10000.0

    ###########################################################################
    #              COMPUTE PAYOFF GIVEN PATHS                                 #
    ###########################################################################
    def calculate_payoff(self, dict_paths):

        for underlyer in self.list_requiredUnderlyers:
            df_path = dict_paths[underlyer]

            # pick the underlying timepoint for payoff evaluation
            arr_underlying = df_path.iloc[self.nTradingDaysRHP-1]

            # note:
            # applying the max(arg, 0) function won't work, because it is not a vectorized operation
            # instead, we calculate the argument of max(arg, 0),
            # and set all values < 0 to 0

            # EURUSD, EURGBP, USDINR, ...
            if self.ccy_SET == self.ccy_FOR:
                if self.positionType == 'long' and self.optionType  == 'Call':
                    #arr_payoff = max(self.CallAmount / arr_underlying * (arr_underlying - self.strike), 0.0)
                    arr_payoff = self.CallAmount / arr_underlying * (arr_underlying - self.strike)
                    arr_payoff[arr_payoff < 0.0] = 0.0
                elif self.positionType == 'long' and self.optionType  == 'Put':
                    #arr_payoff = max((-1)*self.CallAmount / arr_underlying * (arr_underlying - self.strike), 0.0)
                    arr_payoff = (-1)*self.CallAmount / arr_underlying * (arr_underlying - self.strike)
                    arr_payoff[arr_payoff < 0.0] = 0.0
                elif self.positionType == 'short' and self.optionType  == 'Call':
                    #arr_payoff = (-1)*max(self.CallAmount / arr_underlying * (arr_underlying - self.strike), 0.0)
                    arr_payoff = self.CallAmount / arr_underlying * (arr_underlying - self.strike)
                    arr_payoff[arr_payoff < 0.0] = 0.0
                    arr_payoff = arr_payoff* (-1)
                elif self.positionType == 'short' and self.optionType  == 'Put':
                    #arr_payoff = (-1)*max((-1)*self.CallAmount / arr_underlying * (arr_underlying - self.strike), 0.0)
                    arr_payoff = (-1)*self.CallAmount / arr_underlying * (arr_underlying - self.strike)
                    arr_payoff[arr_payoff < 0.0] = 0.0
                    arr_payoff = arr_payoff* (-1)
                else:
                    arr_payoff = np.NaN

            # XAUUSD
            elif self.ccy_SET == self.ccy_DOM:
                if self.positionType == 'long' and self.optionType  == 'Call':
                    #arr_payoff = max(self.CallAmount * (arr_underlying - self.strike), 0.0)
                    arr_payoff = self.CallAmount * (arr_underlying - self.strike)
                    arr_payoff[arr_payoff < 0.0] = 0.0
                elif self.positionType == 'long' and self.optionType  == 'Put':
                    #arr_payoff = max((-1)*self.CallAmount * (arr_underlying - self.strike), 0.0)
                    arr_payoff = (-1)*self.CallAmount * (arr_underlying - self.strike)
                    arr_payoff[arr_payoff < 0.0] = 0.0
                elif self.positionType == 'short' and self.optionType  == 'Call':
                    #arr_payoff = (-1)*max(self.CallAmount * (arr_underlying - self.strike), 0.0)
                    arr_payoff = self.CallAmount * (arr_underlying - self.strike)
                    arr_payoff[arr_payoff < 0.0] = 0.0
                    arr_payoff = arr_payoff* (-1)
                elif self.positionType == 'short' and self.optionType  == 'Put':
                    #arr_payoff = (-1)*max((-1)*self.CallAmount * (arr_underlying - self.strike), 0.0)
                    arr_payoff = (-1)*self.CallAmount * (arr_underlying - self.strike)
                    arr_payoff[arr_payoff < 0.0] = 0.0
                    arr_payoff = arr_payoff* (-1)
                else:
                    arr_payoff = np.NaN
            return arr_payoff

    ###########################################################################
    #              PRODUCE RiskEngineOutput.csv                               #
    ###########################################################################
    def produce_REO_output(self):
        line_start = "FX-Option-"
        if self.deliveryType == "deliverable":
            line_start += "Deliverable-European-"
        else:
            line_start +=  "Non-Deliverable-European-"
        line_start += self.ccy_FOR + "-" + self.ccy_DOM + "-"
        if self.positionType == "long":
            line_start += "Long-"
        else:
            line_start +=  "Short-"
        if self.optionType == "Call":
            line_start += "Call-"
        else:
            line_start += "Put-"
        line_start += str(self.product_id)

        new_row = { \
                     'id'               : line_start ,
                     'MRM'              : self.MRM,
                     'CRM'              : self.CRM,
                     'VAR'              : 0,
                     'VEV'              : 0,
                     'HistoricalVEV'    : 0,
                     'SRI'              : self.SRI,
                     'InvestmentAmount' : self.OptionValue,
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

        Some notes:
         [1] The Call/Put Amounts are obtained by multiplying by the ScalingFactor
         [2] The CallAmount/PutAmount switch place for Put options! (WTF?)
         [3] There seems to be DCI/Barrier Input related to European Options
         [4] There are few weird dates included (SettlementDateWithLag)
         [5] ReportingCurrency / SecondCurrency independent of Put/Call

        Output:
        a list of lines that need to be written into Attributes.txt
        """

        #---------------------------------------------------------------------#
        # construct String at beginning of each line                          #
        #---------------------------------------------------------------------#
        line_start = "FX-Option-"
        if self.deliveryType == "deliverable":
            line_start += "Deliverable-European-"
        else:
            line_start +=  "Non-Deliverable-European-"
        line_start += self.ccy_FOR + "-" + self.ccy_DOM + "-"
        if self.positionType == "long":
            line_start += "Long-"
        else:
            line_start +=  "Short-"
        if self.optionType == "Call":
            line_start += "Call-"
        else:
            line_start += "Put-"
        line_start += str(self.product_id)

        #---------------------------------------------------------------------#
        # collect all information needed in dictionary                        #
        #---------------------------------------------------------------------#
        dict_attributes = dict()

        dict_attributes["LineStart"]    = line_start

        if self.optionType == "Call":
            dict_attributes["CallAmount"]    = str(self.CallAmount*self.ScalingFactor)
            dict_attributes["CallCCY"]  = str(self.CallCurrency)
        elif self.optionType == "Put":
            dict_attributes["CallAmount"]    = str(self.PutAmount*self.ScalingFactor)
            dict_attributes["CallCCY"]  = str(self.PutCurrency)
        else:
            print('[ERROR - produce_Attributes_output]. ')
            print('         optionType can only be Call or Put')
            print('         Found: ', self.optionType)
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')
            self.flag_error_encountered = True

        # TODO - sort this one out
        dict_attributes["EffectiveDate"] = convertDateFormatForAttributes(self.tradeDate)
        if dict_attributes["EffectiveDate"] == None:
            self.flag_error_encountered = True
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')

        # TODO - sort this one out
        dict_attributes["ExpiryDate"] = convertDateFormatForAttributes(self.fixingDate)
        if dict_attributes["ExpiryDate"] == None:
            self.flag_error_encountered = True
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')

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

        dict_attributes["LowerTriggerRate"] = "N/A"

        # TODO - sort this one out
        dict_attributes["PremiumPaymentDate"] = convertDateFormatForAttributes(self.tradeDate)
        if dict_attributes["PremiumPaymentDate"] == None:
            self.flag_error_encountered = True
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')

        if self.deliveryType == "deliverable":
            dict_attributes["ProductName"] = "Over the Counter Deliverable Currency Option"
        elif self.deliveryType == "nondeliverable":
            dict_attributes["ProductName"] = "Over the Counter Non-Deliverable Currency Option"
        else:
            print('[ERROR - produce_Attributes_output]. ')
            print('         DeliveryType can only be deliverable or nondeliverable')
            print('         Found: ', self.deliveryType)
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')
            self.flag_error_encountered = True

        if self.optionType == "Call":
            dict_attributes["PutAmount"] = str(self.PutAmount*self.ScalingFactor)
            dict_attributes["PutCCY"]    = str(self.PutCurrency)
        elif self.optionType == "Put":
            dict_attributes["PutAmount"] = str(self.CallAmount*self.ScalingFactor)
            dict_attributes["PutCCY"]    = str(self.CallCurrency)
        else:
            print('[ERROR - produce_Attributes_output]. ')
            print('         optionType can only be Call or Put')
            print('         Found: ', self.optionType)
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')
            self.flag_error_encountered = True

        if self.optionType == "Call":
            dict_attributes["PutCall"] = "Call"
        elif self.optionType == "Put":
            dict_attributes["PutCall"] = "Put"
        else:
            print('[ERROR - produce_Attributes_output]. ')
            print('         optionType can only be Call or Put')
            print('         Found: ', self.optionType)
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')
            self.flag_error_encountered = True

        dict_attributes["ReportingCurrency"] = str(self.ccy_SET)

        dict_attributes["SecondCurrency"] = str(self.ccy_DOM)

        dict_attributes["SettlementDate"] = convertDateFormatForAttributes(self.settlementDate)
        if dict_attributes["SettlementDate"] == None:
            self.flag_error_encountered = True
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')

        # TODO: wtf is this?
        dict_attributes["SettlementDateWithLag"] = convertDateFormatForAttributes(self.settlementDate)
        if dict_attributes["SettlementDateWithLag"] == None:
            self.flag_error_encountered = True
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')

        # TODO - sort this one out
        dict_attributes["SettlementExpiration"] = "expiration"

        subtype = "FX-Option-"
        if self.deliveryType == "deliverable":
            line_start += "Deliverable-European-"
        else:
            line_start +=  "Non-Deliverable-European-"
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

        dict_attributes["TriggerRate"] = "N/A"

        dict_attributes["UpperTriggerRate"] = "N/A"

        return dict_attributes
