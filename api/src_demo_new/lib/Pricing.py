import numpy as np
import scipy.stats as ss

def value_FX_Option_GarmanKohlhagen( spot, 
                                     strike, 
                                     volatility, 
                                     yield_FOR, 
                                     yield_DOM, 
                                     T,
                                     ccy_FOR,
                                     ccy_DOM,
                                     ccy_SET,
                                     optionType,
                                     CallAmount):
                                     
        d1 =  (np.log(spot/strike) + (yield_DOM - yield_FOR + volatility**2 / 2) * T) / \
              (volatility * np.sqrt(T))
        d2 =  d1 - volatility*np.sqrt(T)

        #---------------------------------------------------------------------#
        #          Call option                                                #
        #---------------------------------------------------------------------#
        if optionType  == 'Call':
            # option value for one unit of FOR currency
            value = spot * np.exp(-yield_FOR*T) * ss.norm.cdf(d1) - \
                    strike * np.exp(-yield_DOM*T) * ss.norm.cdf(d2)

            # EURUSD, EURGBP, ...
            if ccy_SET == ccy_FOR:
                value = value * CallAmount / spot

            # XAUUSD
            elif ccy_SET == ccy_DOM:
                value = value * CallAmount

            else:
                print('[ERROR]. The settlement currency must be either FOR or DOM.')
                print('         ccy_FOR: ' + ccy_FOR)
                print('         ccy_DOM: ' + ccy_DOM)
                print('         ccy_SET: ' + ccy_SET)
                return None

            return value
        
        #---------------------------------------------------------------------#
        #          Put option                                                 #
        #---------------------------------------------------------------------#
        elif optionType  == 'Put':
            # option value for one unit of FOR currency
            value = strike * np.exp(-yield_DOM*T) * ss.norm.cdf(-d2) - \
                    spot * np.exp(-yield_FOR*T) * ss.norm.cdf(-d1) 

            # EURUSD, EURGBP, ...
            if ccy_SET == ccy_FOR:
                value = value * CallAmount / spot

            # XAUUSD
            elif ccy_SET == ccy_DOM:
                value = value * CallAmount

            else:
                print('[ERROR]. The settlement currency must be either FOR or DOM.')
                print('         ccy_FOR: ' + ccy_FOR)
                print('         ccy_DOM: ' + ccy_DOM)
                print('         ccy_SET: ' + ccy_SET)
                return None

            return value
        else:
            print('[ERROR]. Unknown option type. Must be Call or Put, but received: ' + optionType )
            return None
 
############################################################################### 
#        Calculate Strike & Vol of ItM / OtM Options                          #
###############################################################################  
class class_FX_Vol_Malz:
    def __init__(self , ccy_FOR, ccy_DOM, RR_25, MS_25 , VolAtm , DeltaFlag):

        self.ccy_FOR    = ccy_FOR
        self.ccy_DOM    = ccy_DOM
        self.RR_25      = RR_25
        self.MS_25      = MS_25
        self.VolAtm     = VolAtm
        self.DeltaFlag  = DeltaFlag

        self.beta0 = 1
        self.beta1 = -2
        self.beta2 = 16

    ############################################################################### 
    #        Get Volatility for Delta                                             #
    ###############################################################################
    def MalzGetVol(self, RR_25, MS_25,VolAtm, delta):

        Vol = self.beta0 * VolAtm + self.beta1 * RR_25 * (delta / 100 - 0.5) + self.beta2 * MS_25 * ((delta / 100 - 0.5) ** 2)
        return Vol

    ############################################################################### 
    #        Get Strike for SpotDelta                                             #
    ###############################################################################
    def MalzGetStrikeForSpotDelta(self, Spot, Vol, T, Rd, Rf, delta):
        v_delta = delta
        phi= 0
        alpha= 0

        #---------------------------------------------------------------------#
        #          AtM                                                        #
        #---------------------------------------------------------------------#
        if delta == 50:
            Strike = Spot * np.exp((Rd - Rf) * T) * np.exp(0.5 * Vol * Vol * T)

        #---------------------------------------------------------------------#
        #          Call Option                                                #
        #---------------------------------------------------------------------#
        elif delta < 50:
            phi = 1
            alpha= ss.norm.ppf(phi * v_delta / 100 * np.exp(Rf * T))
            Strike = Spot * np.exp((Rd - Rf) * T) * np.exp(-phi * alpha * Vol * (T ** 0.5) + (0.5 * Vol * Vol * T))
        
        #---------------------------------------------------------------------#
        #          Put Option                                                 #
        #---------------------------------------------------------------------#
        else:
            v_delta = -(100 - delta)
            phi = -1
            alpha = ss.norm.ppf(phi * v_delta / 100 * np.exp(Rf * T))
            Strike= Spot * np.exp((Rd - Rf) * T) * np.exp(-phi * alpha * Vol * (T ** 0.5) + (0.5 * Vol * Vol * T))

        return Strike

    ############################################################################### 
    #        Get Strike for ForwardDelta                                          #
    ###############################################################################
    def MalzGetStrikeForForwardDelta(self, Spot, Vol, T, Rd, Rf, delta):
        v_delta = delta
        phi= 0
        alpha= 0

        #---------------------------------------------------------------------#
        #          AtM                                                        #
        #---------------------------------------------------------------------#
        if delta== 50:
            Strike = Spot * np.exp((Rd - Rf) * T) * np.exp(0.5 * Vol * Vol * T)

        #---------------------------------------------------------------------#
        #          Call Option                                                #
        #---------------------------------------------------------------------#
        elif delta < 50:
            phi = 1
            alpha= ss.norm.ppf(phi * v_delta / 100) 
            Strike = Spot * np.exp((Rd - Rf) * T) * np.exp(-phi * alpha * Vol * (T ** 0.5) + (0.5 * Vol * Vol * T))
        
        #---------------------------------------------------------------------#
        #          Put Option                                                 #
        #---------------------------------------------------------------------#
        else:
            v_delta = -(100 - delta)
            phi = -1
            alpha = ss.norm.ppf(phi * v_delta / 100)
            Strike = Spot * np.exp((Rd - Rf) * T) * np.exp(-phi * alpha * Vol * (T ** 0.5) + (0.5 * Vol * Vol * T))

        return Strike

    ############################################################################### 
    #        Get Strike for Delta                                                 #
    ###############################################################################
    def MalzGetStrikeForDelta(self, DeltaFlag, Spot, Vol, T, Rd, Rf, delta, TDays):

        #TDays - Maturity in days
        #T - maturity in time fraction

        if self.DeltaFlag == 'SPOT' :
            StrikeForDelta = self.MalzGetStrikeForSpotDelta(Spot, Vol, T, Rd, Rf, delta)

        elif self.DeltaFlag == 'FORWARD':
            StrikeForDelta = self.MalzGetStrikeForForwardDelta(Spot, Vol, T, Rd, Rf, delta)

        elif self.DeltaFlag == 'SPOT-719-FORWARD':
            if TDays <= 719:
                StrikeForDelta = self.MalzGetStrikeForSpotDelta(Spot, Vol, T, Rd, Rf, delta)
            else:
                StrikeForDelta = self.MalzGetStrikeForForwardDelta(Spot, Vol, T, Rd, Rf, delta)

        return StrikeForDelta