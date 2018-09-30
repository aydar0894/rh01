import numpy as np
import scipy.stats as ss
from .PreProcessing import calculate_strike, \
                              get_SpotRate    , \
                              get_Yield       , \
                              get_FXATMVol    , \
                              get_FX_MS_Vol   , \
                              get_FX_RR_Vol
from .DateLogic import getYearFraction                , \
                          addTenor                       , \
                          networkdays                    , \
                          getHolidaysBetween             , \
                          convertDateFormatForAttributes , \
                          getTenorInformation

from .Pricing import value_FX_Option_GarmanKohlhagen
from .Pricing import class_FX_Vol_Malz



class class_FX_DCI:
    def __init__(self,
                 ccy_FOR         ,
                 ccy_DOM         ,
                 ccy_SET         ,
                 CallCurrency    ,
                 CallAmount      ,
                 PutCurrency     ,
                 PutAmount       ,
                 RHP_string      ,
                 DCIType         ,
                 cost_input_perc ,
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
        self.DCIType         = DCIType
        self.cost_input_perc = cost_input_perc
        self.MRM             = MRM
        self.SRI             = SRI
        self.CRM             = CRM

        self.product_type    = 'FX_DCI'
        self.product_id      = None
        self.optionType      = 'Call'  # DCIs are also short Call options
        self.positionType    = 'short' # DCIs are also short Call options

        self.arr_payoffs_FMU = np.array([]) # payoffs from Favourable, Moderate and Unfavourable Scenario
        self.arr_payoffs_S   = np.array([]) # payoffs from Stressed Scenario

        # intermediate results
        # GrossPerformance amounts of short option are computed first
        # they are then used to calculate DCI GrossPerformance
        self.GrossAmount_Favourable_ShortOption   = None
        self.GrossAmount_Moderate_ShortOption     = None
        self.GrossAmount_Unfavourable_ShortOption = None
        self.GrossAmount_Stressed_ShortOption     = None

        self.GrossAmount_Favourable   = None
        self.GrossAmount_Moderate     = None
        self.GrossAmount_Unfavourable = None
        self.GrossAmount_Stressed     = None

        # not needed for any intermediate calculations, but part of REO
        self.ScalingFactor = 1.0
        self.Cost     = 10000.0 * self.cost_input_perc

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
                      df_CurrentMarketData_Exchange   ,
                      df_CurrentMarketData_Yield      ,
                      dict_YieldCcy_CurveName         ,
                      df_CurrentMarketData_FXATMVol   ,
                      dict_FXATMVol_CurveName         ,
                      df_CurrentMarketData_FXDeltaVol ,
                      dict_FXDeltaVol_MS_CurveName    ,
                      dict_FXDeltaVol_RR_CurveName    ):

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
        #          calculate 40 Delta strike                                  #
        #.....................................................................#
        # get the counter currency for vol surface data
        if self.ccy_FOR == 'XAU':
            ccy_name_counter = 'XAU'
        else:
            ccy_name_counter = self.ccy_DOM

        #...............#
        # get tenor [d] #
        #...............#
        tenor_in_days = int(self.T_RHP * 360)

        #...............#
        # get spot_rate #
        #...............#
        spot_rate = get_SpotRate(df_CurrentMarketData_Exchange = df_CurrentMarketData_Exchange, \
                                 ccy_FOR                       = self.ccy_FOR                 , \
                                 ccy_DOM                       = self.ccy_DOM                 )

        # check if data was found
        if spot_rate == None:
            print('[ERROR - DCI]. Could not find spot_rate.')
            print('               ', ccy_FOR, ccy_DOM)
            print('               This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1
        self.spot_rate = spot_rate

        #...............#
        # get yield_FOR #
        #...............#
        yield_FOR = get_Yield( df_CurrentMarketData_Yield = df_CurrentMarketData_Yield , \
                               dict_YieldCcy_CurveName    = dict_YieldCcy_CurveName    , \
                               tenor_in_days              = tenor_in_days              , \
                               ccy_pair                   = self.ccy_FOR+self.ccy_DOM  , \
                               ccy_name                   = self.ccy_FOR               )
        self.yield_FOR = yield_FOR

        #...............#
        # get yield_DOM #
        #...............#
        yield_DOM = get_Yield( df_CurrentMarketData_Yield = df_CurrentMarketData_Yield , \
                               dict_YieldCcy_CurveName    = dict_YieldCcy_CurveName    , \
                               tenor_in_days              = tenor_in_days              , \
                               ccy_pair                   = self.ccy_FOR+self.ccy_DOM  , \
                               ccy_name                   = self.ccy_DOM               )
        self.yield_DOM = yield_DOM

        #...............#
        # get vol_ATM   #
        #...............#
        volatilityATM = get_FXATMVol( df_CurrentMarketData_FXATMVol = df_CurrentMarketData_FXATMVol , \
                                      dict_FXATMVol_CurveName       = dict_FXATMVol_CurveName       , \
                                      tenor_in_days                 = tenor_in_days                 , \
                                      ccy_name                      = self.ccy_FOR                  , \
                                      ccy_name_counter              = ccy_name_counter              )
        self.volatility_ATM = volatilityATM

        #....................#
        # get vol_Delta (MS) #
        #....................#
        # get volatility for MarketStrangle and RiskReversal from MarketData
        # since we are only interested in 40Delta options, we only need the 25Delta point for both
        # TODO: add interpolation between maturity points if necessary

        volatility_MS_25Delta, DeltaFlag_MS = get_FX_MS_Vol( df_CurrentMarketData_FXDeltaVol = df_CurrentMarketData_FXDeltaVol, \
                                                             dict_FXDeltaVol_CurveName       = dict_FXDeltaVol_MS_CurveName   , \
                                                             tenor_in_days                   = tenor_in_days                  , \
                                                             ccy_name                        = self.ccy_FOR                   , \
                                                             ccy_name_counter                = ccy_name_counter               , \
                                                             DeltaValue                      = 0.25                             )
        self.volatility_MS_25Delta = volatility_MS_25Delta

        #....................#
        # get vol_Delta (RR) #
        #....................#
        volatility_RR_25Delta, DeltaFlag_RR = get_FX_RR_Vol( df_CurrentMarketData_FXDeltaVol = df_CurrentMarketData_FXDeltaVol, \
                                                             dict_FXDeltaVol_CurveName       = dict_FXDeltaVol_RR_CurveName   , \
                                                             tenor_in_days                   = tenor_in_days                  , \
                                                             ccy_name                        = self.ccy_FOR                   , \
                                                             ccy_name_counter                = ccy_name_counter               , \
                                                             DeltaValue                      = 0.25                             )
        self.volatility_RR_25Delta = volatility_RR_25Delta

        #....................#
        # check DeltaFlag    #
        #....................#
        if DeltaFlag_MS != DeltaFlag_RR:
            print('[ERROR - DCI]. The DeltaFlag for RiskReversal and MarketStrangle are different.')
            print('               ', self.ccy_FOR, self.ccy_DOM, 'tenor: ', tenor_in_days, DeltaFlag_MS, DeltaFlag_RR)
            print('               This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1

        DeltaFlag = DeltaFlag_MS

        if DeltaFlag not in ['SPOT', 'FORWARD', 'SPOT-719-FORWARD']:
            print('[ERROR - DCI]. The DeltaFlag value not recognized. Allowed values: SPOT/FORWARD/SPOT-719-FORWARD')
            print('               ', self.ccy_FOR, self.ccy_DOM, 'tenor: ', tenor_in_days, DeltaFlag)
            print('               This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1
        self.DeltaFlag = DeltaFlag

        #........................#
        # calculate 40-Delta Vol #
        #........................#
        MalzCalculator = class_FX_Vol_Malz( ccy_FOR   = self.ccy_FOR          ,
                                            ccy_DOM   = self.ccy_DOM          ,
                                            RR_25     = volatility_RR_25Delta ,
                                            MS_25     = volatility_MS_25Delta ,
                                            VolAtm    = volatilityATM         ,
                                            DeltaFlag = DeltaFlag             )

        self.volatility_40Delta = MalzCalculator.MalzGetVol( RR_25  = volatility_RR_25Delta ,
                                                             MS_25  = volatility_MS_25Delta ,
                                                             VolAtm = volatilityATM         ,
                                                             delta  = 40                    )

        #...........................#
        # calculate 40-Delta Strike #
        #...........................#
        self.strike = MalzCalculator.MalzGetStrikeForDelta( DeltaFlag = DeltaFlag               ,
                                                            Spot      = spot_rate               ,
                                                            Vol       = self.volatility_40Delta ,
                                                            T         = self.T_RHP              ,
                                                            Rd        = yield_DOM               ,
                                                            Rf        = yield_FOR               ,
                                                            delta     = 40                      ,
                                                            TDays     = self.nTradingDaysRHP    )

        #.....................................................................#
        #          calculate CallAmount & PutAmount                           #
        #.....................................................................#
        # sanity check: currency denomination Call/Put currency
        if self.CallCurrency != self.ccy_FOR:
            print('[ERROR - DCI]. CallCurrency is not the same as FOR currency.')
            print('               This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1
        if self.PutCurrency != self.ccy_DOM:
            print('[ERROR - DCI]. PutCurrency is not the same as DOM currency.')
            print('               This cannot happen with products in scope. Skipping...')
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
            print('[ERROR - DCI]. PutAmount / CallAmount not set correctly.')
            print('               Should be 10,000 and None, but was:')
            print('                 CallAmount: ' + str(self.CallAmount))
            print('                 PutAmount:  ' + str(self.PutAmount))

            print('         This cannot happen with products in scope. Skipping...')
            self.flag_error_encountered = True
            return -1

        #.....................................................................#
        #          store deposit rate                                         #
        #.....................................................................#
        # EURUSD, GBPUSD, ...
        if self.ccy_SET == self.ccy_FOR:
            self.DepositRate = yield_FOR

        # XAUUSD
        elif self.ccy_SET == self.ccy_DOM:
            self.DepositRate = yield_DOM

        else:
            print('[ERROR - DCI]. DepositRate could not be set. FOR, DOM and SET currency issues...')
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

        # DCI rely on shorting a Call option (40 Delta)
        OptionValue = value_FX_Option_GarmanKohlhagen( spot       = spot_rate               , \
                                                       strike     = self.strike             , \
                                                       volatility = self.volatility_40Delta , \
                                                       yield_FOR  = yield_FOR               , \
                                                       yield_DOM  = yield_DOM               , \
                                                       T          = self.T_RHP              , \
                                                       ccy_FOR    = self.ccy_FOR            , \
                                                       ccy_DOM    = self.ccy_DOM            , \
                                                       ccy_SET    = self.ccy_SET            , \
                                                       optionType = self.optionType         , \
                                                       CallAmount = self.CallAmount         )

        if OptionValue == None:
            self.flag_error_encountered = True
            return -1
        self.OptionValue = OptionValue

        #.....................................................................#
        #          calculate the Enhanced Coupon Rate                         #
        #.....................................................................#
        if self.DCIType == 'InterestNotAtRisk':
            self.OptionPremiumPerc     = self.OptionValue / 10000.0
            self.GrossEnhancedCoupon   = self.DepositRate*self.T_RHP + self.OptionPremiumPerc
            self.EnhancedCoupon        = self.GrossEnhancedCoupon - self.cost_input_perc
            self.GrossEnhancedNotional = 10000.0
            self.EnhancedNotional      = 10000.0

        elif self.DCIType == 'InterestAtRisk':
            self.OptionPremiumPerc   = self.OptionValue / 10000.0
            self.GrossEnhancedCoupon = self.DepositRate*self.T_RHP + self.OptionPremiumPerc * (1.0 + self.DepositRate*self.T_RHP + self.OptionPremiumPerc)
            self.EnhancedCoupon      = self.GrossEnhancedCoupon - self.cost_input_perc
            self.GrossEnhancedNotional = 10000.0 * (1.0 + self.GrossEnhancedCoupon)
            self.EnhancedNotional      = 10000.0 * (1.0 + self.EnhancedCoupon)

        else:
            print('[ERROR - DCI]. The only DCITypes that are recognised are InterestNotAtRisk and InterestAtRisk.')
            print('               DCIType: ', self.DCIType)
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
        self.GrossAmount_Favourable_ShortOption   = np.percentile(self.arr_payoffs_FMU, 90.0)
        self.GrossAmount_Moderate_ShortOption     = np.percentile(self.arr_payoffs_FMU, 50.0)
        self.GrossAmount_Unfavourable_ShortOption = np.percentile(self.arr_payoffs_FMU, 10.0)

        #---------------------------------------------------------------------#
        #          Interest Not At Risk                                       #
        #---------------------------------------------------------------------#
        if self.DCIType == 'InterestNotAtRisk':
            self.GrossAmount_Favourable   = 10000.0 * (1.0 + self.GrossEnhancedCoupon + self.GrossAmount_Favourable_ShortOption   / self.GrossEnhancedNotional)
            self.GrossAmount_Moderate     = 10000.0 * (1.0 + self.GrossEnhancedCoupon + self.GrossAmount_Moderate_ShortOption     / self.GrossEnhancedNotional)
            self.GrossAmount_Unfavourable = 10000.0 * (1.0 + self.GrossEnhancedCoupon + self.GrossAmount_Unfavourable_ShortOption / self.GrossEnhancedNotional)

        #---------------------------------------------------------------------#
        #          Interest At Risk                                           #
        #---------------------------------------------------------------------#
        elif self.DCIType == 'InterestAtRisk':
            self.GrossAmount_Favourable   = 10000.0 * (1.0 + self.GrossEnhancedCoupon) + self.GrossAmount_Favourable_ShortOption   * self.GrossEnhancedNotional / 10000.0
            self.GrossAmount_Moderate     = 10000.0 * (1.0 + self.GrossEnhancedCoupon) + self.GrossAmount_Moderate_ShortOption     * self.GrossEnhancedNotional / 10000.0
            self.GrossAmount_Unfavourable = 10000.0 * (1.0 + self.GrossEnhancedCoupon) + self.GrossAmount_Unfavourable_ShortOption * self.GrossEnhancedNotional / 10000.0

        #---------------------------------------------------------------------#
        #          Unknown DCI Type                                           #
        #---------------------------------------------------------------------#
        else:
            print('[ERROR - DCI]. The only DCITypes that are recognised are InterestNotAtRisk and InterestAtRisk.')
            print('               DCIType: ', self.DCIType)
            self.flag_error_encountered = True

    ###########################################################################
    #              GROSS AMOUNTS STRESSED                                     #
    ###########################################################################
    def calculate_GrossAmount_S(self):
        # this is only true for products with tenor <= 1Y
        self.GrossAmount_Stressed_ShortOption = np.percentile(self.arr_payoffs_S, 1.0)

        #---------------------------------------------------------------------#
        #          Interest Not At Risk                                       #
        #---------------------------------------------------------------------#
        if self.DCIType == 'InterestNotAtRisk':
            self.GrossAmount_Stressed   = 10000.0 * (1.0 + self.GrossEnhancedCoupon + self.GrossAmount_Stressed_ShortOption   / self.GrossEnhancedNotional)

        #---------------------------------------------------------------------#
        #          Interest At Risk                                           #
        #---------------------------------------------------------------------#
        elif self.DCIType == 'InterestAtRisk':
            self.GrossAmount_Stressed   = 10000.0 * (1.0 + self.GrossEnhancedCoupon) + self.GrossAmount_Stressed_ShortOption   * self.GrossEnhancedNotional / 10000.0

        #---------------------------------------------------------------------#
        #          Unknown DCI Type                                           #
        #---------------------------------------------------------------------#
        else:
            print('[ERROR - DCI]. The only DCITypes that are recognised are InterestNotAtRisk and InterestAtRisk.')
            print('               DCIType: ', self.DCIType)
            self.flag_error_encountered = True

    ###########################################################################
    #              NET AMOUNTS                                                #
    ###########################################################################
    def calculate_NetAmounts(self):

        self.calculate_fFX()

        #---------------------------------------------------------------------#
        #          Interest Not At Risk                                       #
        #---------------------------------------------------------------------#
        if self.DCIType == 'InterestNotAtRisk':
            self.NetAmount_Favourable   = 10000.0 * (1.0 + self.EnhancedCoupon + self.fFX_Favourable)
            self.NetAmount_Moderate     = 10000.0 * (1.0 + self.EnhancedCoupon + self.fFX_Moderate)
            self.NetAmount_Unfavourable = 10000.0 * (1.0 + self.EnhancedCoupon + self.fFX_Unfavourable)
            self.NetAmount_Stressed     = 10000.0 * (1.0 + self.EnhancedCoupon + self.fFX_Stressed)

        #---------------------------------------------------------------------#
        #          Interest At Risk                                           #
        #---------------------------------------------------------------------#
        elif self.DCIType == 'InterestAtRisk':
            self.NetAmount_Favourable   = 10000.0 * (1.0 + self.EnhancedCoupon) * (1.0 + self.fFX_Favourable)
            self.NetAmount_Moderate     = 10000.0 * (1.0 + self.EnhancedCoupon) * (1.0 + self.fFX_Moderate)
            self.NetAmount_Unfavourable = 10000.0 * (1.0 + self.EnhancedCoupon) * (1.0 + self.fFX_Unfavourable)
            self.NetAmount_Stressed     = 10000.0 * (1.0 + self.EnhancedCoupon) * (1.0 + self.fFX_Stressed)

        #---------------------------------------------------------------------#
        #          Unknown DCI Type                                           #
        #---------------------------------------------------------------------#
        else:
            print('[ERROR - DCI]. The only DCITypes that are recognised are InterestNotAtRisk and InterestAtRisk.')
            print('               DCIType: ', self.DCIType)
            self.flag_error_encountered = True

    ###########################################################################
    #              NET RETURNS                                                #
    ###########################################################################
    def calculate_NetReturns(self):
        #---------------------------------------------------------------------#
        #          Interest Not At Risk                                       #
        #---------------------------------------------------------------------#
        if self.DCIType == 'InterestNotAtRisk':
            self.NetReturn_Favourable   = self.EnhancedCoupon + self.fFX_Favourable
            self.NetReturn_Moderate     = self.EnhancedCoupon + self.fFX_Moderate
            self.NetReturn_Unfavourable = self.EnhancedCoupon + self.fFX_Unfavourable
            self.NetReturn_Stressed     = self.EnhancedCoupon + self.fFX_Stressed

        #---------------------------------------------------------------------#
        #          Interest At Risk                                           #
        #---------------------------------------------------------------------#
        elif self.DCIType == 'InterestAtRisk':
            self.NetReturn_Favourable   = self.EnhancedCoupon + self.fFX_Favourable   * (1.0 + self.EnhancedCoupon)
            self.NetReturn_Moderate     = self.EnhancedCoupon + self.fFX_Moderate     * (1.0 + self.EnhancedCoupon)
            self.NetReturn_Unfavourable = self.EnhancedCoupon + self.fFX_Unfavourable * (1.0 + self.EnhancedCoupon)
            self.NetReturn_Stressed     = self.EnhancedCoupon + self.fFX_Stressed     * (1.0 + self.EnhancedCoupon)

        #---------------------------------------------------------------------#
        #          Unknown DCI Type                                           #
        #---------------------------------------------------------------------#
        else:
            print('[ERROR - DCI]. The only DCITypes that are recognised are InterestNotAtRisk and InterestAtRisk.')
            print('               DCIType: ', self.DCIType)
            self.flag_error_encountered = True


    ###########################################################################
    #              RIY                                                        #
    ###########################################################################
    def calculate_RIYs(self):
        #---------------------------------------------------------------------#
        #          Interest Not At Risk                                       #
        #---------------------------------------------------------------------#
        if self.DCIType == 'InterestNotAtRisk':
            i = np.power((1.0 + self.fFX_Moderate + self.GrossEnhancedCoupon), (1.0/self.T_RHP))
            r = np.power((1.0 + self.fFX_Moderate + self.EnhancedCoupon)     , (1.0/self.T_RHP))

            self.RIYRHP = i - r

        #---------------------------------------------------------------------#
        #          Interest At Risk                                           #
        #---------------------------------------------------------------------#
        elif self.DCIType == 'InterestAtRisk':
            i = np.power((1.0 + self.fFX_Moderate*(1.0+self.GrossEnhancedCoupon) + self.GrossEnhancedCoupon), (1.0/self.T_RHP))
            r = np.power((1.0 + self.fFX_Moderate*(1.0+self.EnhancedCoupon)      + self.EnhancedCoupon)     , (1.0/self.T_RHP))

            self.RIYRHP = i - r

        #---------------------------------------------------------------------#
        #          Unknown DCI Type                                           #
        #---------------------------------------------------------------------#
        else:
            print('[ERROR - DCI]. The only DCITypes that are recognised are InterestNotAtRisk and InterestAtRisk.')
            print('               DCIType: ', self.DCIType)
            self.flag_error_encountered = True

    ###########################################################################
    #              f(FX)                                                      #
    ###########################################################################
    def calculate_fFX(self):
        self.fFX_Favourable   = self.GrossAmount_Favourable_ShortOption   / 10000.0
        self.fFX_Moderate     = self.GrossAmount_Moderate_ShortOption     / 10000.0
        self.fFX_Unfavourable = self.GrossAmount_Unfavourable_ShortOption / 10000.0
        self.fFX_Stressed     = self.GrossAmount_Stressed_ShortOption     / 10000.0

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
                if self.positionType == 'short' and self.optionType  == 'Call':
                    #arr_payoff = (-1)*max(self.CallAmount / arr_underlying * (arr_underlying - self.strike), 0.0)
                    arr_payoff = self.CallAmount / arr_underlying * (arr_underlying - self.strike)
                    arr_payoff[arr_payoff < 0.0] = 0.0
                    arr_payoff = arr_payoff* (-1)
                else:
                    print('[ERROR - DCI]. DCI GrossAmounts need to be based on short Call options.')
                    print('               Current Setup:' , self.positionType, self.optionType)
                    arr_payoff = np.NaN
                    self.flag_error_encountered = True
                    return arr_payoff

            # XAUUSD
            elif self.ccy_SET == self.ccy_DOM:
                if self.positionType == 'short' and self.optionType  == 'Call':
                    #arr_payoff = (-1)*max(self.CallAmount * (arr_underlying - self.strike), 0.0)
                    arr_payoff = self.CallAmount * (arr_underlying - self.strike)
                    arr_payoff[arr_payoff < 0.0] = 0.0
                    arr_payoff = arr_payoff* (-1)
                else:
                    print('[ERROR - DCI]. DCI GrossAmounts need to be based on short Call options.')
                    print('               Current Setup:' , self.positionType, self.optionType)
                    arr_payoff = np.NaN
                    self.flag_error_encountered = True
                    return arr_payoff

            return arr_payoff

    ###########################################################################
    #              PRODUCE RiskEngineOutput.csv                               #
    ###########################################################################
    def produce_REO_output(self):
        line_start = "FX-DCI-"
        if self.DCIType == "InterestAtRisk":
            line_start += "At-Risk-"
        else:
            line_start +=  "Not-At-Risk-"
        line_start += self.ccy_FOR + "-" + self.ccy_DOM + "-"
        line_start +=  "Short-Call-"
        line_start += str(self.product_id)

        new_row = { \
                    'id'                : line_start ,
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
         [1]
         [2]

        Output:
        a list of lines that need to be written into Attributes.txt
        """

        #---------------------------------------------------------------------#
        # construct String at beginning of each line                          #
        #---------------------------------------------------------------------#
        line_start = "FX-DCI-"
        if self.DCIType == "InterestAtRisk":
            line_start += "At-Risk-"
        else:
            line_start +=  "Not-At-Risk-"
        line_start += self.ccy_FOR + "-" + self.ccy_DOM + "-"
        line_start +=  "Short-Call-"
        line_start += str(self.product_id)

        #---------------------------------------------------------------------#
        # collect all information needed in dictionary                        #
        #---------------------------------------------------------------------#
        dict_attributes = dict()

        dict_attributes["LineStart"]    = line_start

        dict_attributes["CallCCY"]  = str(self.CallCurrency)

        if self.DCIType == "InterestAtRisk":
            dict_attributes["DCICallAmount"]    = str(10000.0 * (1.0 + self.GrossEnhancedCoupon))
        else:
            dict_attributes["DCICallAmount"]    = str(10000.0)

        dict_attributes["DCICoupon"] = str(self.GrossEnhancedCoupon)

        dict_attributes["DCIPutAmount"] = str(self.PutAmount)

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

        if self.DCIType == "InterestAtRisk":
            dict_attributes["InterestAtRisk"] = "At-Risk"
        elif self.DCIType == "InterestNotAtRisk":
            dict_attributes["InterestAtRisk"] = "Not-At-Risk"
        else:
            print('[ERROR - produce_Attributes_output]. ')
            print('         DCIType can only be InterestNotAtRisk or InterestAtRisk')
            print('         Found: ', self.DCIType)
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')
            self.flag_error_encountered = True

        if self.ccy_FOR == "XAU":
            dict_attributes["IsReportingCurrencyBullion"] = "TRUE"
        else:
            dict_attributes["IsReportingCurrencyBullion"] = "FALSE"

        if self.ccy_DOM == "XAU":
            dict_attributes["IsSecondaryCurrencyBullion"] = "TRUE"
        else:
            dict_attributes["IsSecondaryCurrencyBullion"] = "FALSE"

        dict_attributes["LowerTriggerRate"] = "N/A"

        dict_attributes["MMRate"] = str(self.DepositRate)

        # TODO - sort this one out
        dict_attributes["PremiumPaymentDate"] = convertDateFormatForAttributes(self.tradeDate)
        if dict_attributes["PremiumPaymentDate"] == None:
            self.flag_error_encountered = True
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')

        if self.DCIType == "InterestAtRisk":
            dict_attributes["ProductName"] = "Dual Currency Investment with interest at risk"
        elif self.DCIType == "InterestNotAtRisk":
            dict_attributes["ProductName"] = "Dual Currency Investment with interest not at risk"
        else:
            print('[ERROR - produce_Attributes_output]. ')
            print('         DCIType can only be InterestNotAtRisk or InterestAtRisk')
            print('         Found: ', self.DCIType)
            print('         product-id ', str(self.product_id))
            print('--- Skipping product ---')
            self.flag_error_encountered = True

        dict_attributes["PutCCY"]    = str(self.PutCurrency)

        dict_attributes["PutCall"] = "Call"

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

        subtype = "FX-DCI-"
        if self.DCIType == "InterestAtRisk":
            line_start += "At-Risk"
        else:
            line_start +=  "Not-At-Risk"
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
