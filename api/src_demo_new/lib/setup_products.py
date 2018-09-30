from .FX_Forward import class_FX_Forward
from .FX_Swap    import class_FX_Swap
from .FX_Option  import class_FX_Option
from .FX_ODF     import class_FX_ODF
from .FX_DCI     import class_FX_DCI
import os

from .readData import read_setup_products

import sys

def setup_products(jsonObj):
    list_products = []
    # script_dir = os.path.dirname(__file__)
    # dict_data_ProductsToRun = read_setup_products(os.path.join(script_dir, filename))

    print('')
    print('[READING PRODUCT DATA]')
    for product_type, list_product_description in jsonObj.items():

        print('  (+) Product type: ' + product_type)

        for product_description in list_product_description:
            flag_skip_product = False

            ###########################################################################
            #             FORWARDS                                                    #
            ###########################################################################
            if product_type == 'FX_Forward':
                obj_NewProduct = class_FX_Forward ( ccy_FOR         = product_description["ccy_FOR"]         ,
                                                    ccy_DOM         = product_description["ccy_DOM"]         ,
                                                    ccy_SET         = product_description["ccy_SET"]         ,
                                                    ReceiveCurrency = product_description["ReceiveCurrency"] ,
                                                    ReceiveAmount   = product_description["ReceiveAmount"]   ,
                                                    PayCurrency     = product_description["PayCurrency"]     ,
                                                    PayAmount       = product_description["PayAmount"]       ,
                                                    RHP_string      = product_description["T_RHP"]           ,
                                                    positionType    = product_description["positionType"]    ,
                                                    cost_input_perc = product_description["cost_input_perc"] ,
                                                    deliveryType    = product_description["deliveryType"]    ,
                                                    MRM             = product_description["MRM"]             ,
                                                    SRI             = product_description["SRI"]             ,
                                                    CRM             = product_description["CRM"]             )


            ###########################################################################
            #             SWAPS                                                       #
            ###########################################################################
            elif product_type == 'FX_Swap':
                obj_NewProduct = class_FX_Swap ( ccy_FOR                = product_description["ccy_FOR"]                ,
                                                 ccy_DOM                = product_description["ccy_DOM"]                ,
                                                 ccy_SET                = product_description["ccy_SET"]                ,
                                                 ReceiveCurrencyNearLeg = product_description["ReceiveCurrencyNearLeg"] ,
                                                 ReceiveAmountNearLeg   = product_description["ReceiveAmountNearLeg"]   ,
                                                 PayCurrencyNearLeg     = product_description["PayCurrencyNearLeg"]     ,
                                                 PayAmountNearLeg       = product_description["PayAmountNearLeg"]       ,
                                                 ReceiveCurrencyFarLeg  = product_description["ReceiveCurrencyFarLeg"]  ,
                                                 ReceiveAmountFarLeg    = product_description["ReceiveAmountFarLeg"]    ,
                                                 PayCurrencyFarLeg      = product_description["PayCurrencyFarLeg"]      ,
                                                 PayAmountFarLeg        = product_description["PayAmountFarLeg"]        ,
                                                 RHP_string             = product_description["T_RHP"]                  ,
                                                 positionType           = product_description["positionType"]           ,
                                                 cost_input_perc        = product_description["cost_input_perc"]        ,
                                                 MRM                    = product_description["MRM"]                    ,
                                                 SRI                    = product_description["SRI"]                    ,
                                                 CRM                    = product_description["CRM"]                    )



            ###########################################################################
            #             OPTIONS                                                     #
            ###########################################################################
            elif product_type == 'FX_Option':
                obj_NewProduct = class_FX_Option ( ccy_FOR         = product_description["ccy_FOR"]         ,
                                                   ccy_DOM         = product_description["ccy_DOM"]         ,
                                                   ccy_SET         = product_description["ccy_SET"]         ,
                                                   CallCurrency    = product_description["CallCurrency"]    ,
                                                   CallAmount      = product_description["CallAmount"]      ,
                                                   PutCurrency     = product_description["PutCurrency"]     ,
                                                   PutAmount       = product_description["PutAmount"]       ,
                                                   RHP_string      = product_description["T_RHP"]           ,
                                                   positionType    = product_description["positionType"]    ,
                                                   optionType      = product_description["optionType"]      ,
                                                   cost_input_perc = product_description["cost_input_perc"] ,
                                                   deliveryType    = product_description["deliveryType"]    ,
                                                   MRM             = product_description["MRM"]             ,
                                                   SRI             = product_description["SRI"]             ,
                                                   CRM             = product_description["CRM"]             )


            ###########################################################################
            #             DCIs                                                        #
            ###########################################################################
            elif product_type == 'FX_DCI':
                obj_NewProduct = class_FX_DCI    ( ccy_FOR         = product_description["ccy_FOR"]         ,
                                                   ccy_DOM         = product_description["ccy_DOM"]         ,
                                                   ccy_SET         = product_description["ccy_SET"]         ,
                                                   CallCurrency    = product_description["CallCurrency"]    ,
                                                   CallAmount      = product_description["CallAmount"]      ,
                                                   PutCurrency     = product_description["PutCurrency"]     ,
                                                   PutAmount       = product_description["PutAmount"]       ,
                                                   RHP_string      = product_description["T_RHP"]           ,
                                                   DCIType         = product_description["DCIType"]         ,
                                                   cost_input_perc = product_description["cost_input_perc"] ,
                                                   MRM             = product_description["MRM"]             ,
                                                   SRI             = product_description["SRI"]             ,
                                                   CRM             = product_description["CRM"]             )

            ###########################################################################
            #             ODFs                                                        #
            ###########################################################################
            elif product_type == 'FX_ODF':
                obj_NewProduct = class_FX_ODF ( ccy_FOR         = product_description["ccy_FOR"]         ,
                                                ccy_DOM         = product_description["ccy_DOM"]         ,
                                                ccy_SET         = product_description["ccy_SET"]         ,
                                                ReceiveCurrency = product_description["ReceiveCurrency"] ,
                                                ReceiveAmount   = product_description["ReceiveAmount"]   ,
                                                PayCurrency     = product_description["PayCurrency"]     ,
                                                PayAmount       = product_description["PayAmount"]       ,
                                                T_Inter_string  = product_description["T_Intermediate"]  ,
                                                RHP_string      = product_description["T_RHP"]           ,
                                                positionType    = product_description["positionType"]    ,
                                                cost_input_perc = product_description["cost_input_perc"] ,
                                                deliveryType    = product_description["deliveryType"]    ,
                                                MRM             = product_description["MRM"]             ,
                                                SRI             = product_description["SRI"]             ,
                                                CRM             = product_description["CRM"]             )

            ###########################################################################
            #             UNKNOWN PRODUCT TYPE                                        #
            ###########################################################################
            else:
                print('[ERROR]. The product type', product_type, 'is not known.')
                print('         This needs to be one of:')
                print('           FX_Forward')
                print('           FX_Swap')
                print('           FX_Option')
                print('           FX_DCI')
                print('           FX_ODF')
                flag_skip_product = True

            if flag_skip_product == False:
                list_products.append(obj_NewProduct)

    return list_products
