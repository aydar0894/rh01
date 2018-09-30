import json
import numpy as np
import pandas as pd
import os

###########################################################################
#              EXTRACT ALL ATTRIBUTES OF CLASS INSTANCE                   #
###########################################################################
def getAllClassAttributes( class_instance ):
    dict_output = dict()

    dict_attributes = dir(class_instance)

    list_attrs = [attr for attr in dict_attributes if not attr.startswith('__') ]

    for attr in list_attrs:
        # don't write callable objects into the output file
        if not callable(getattr(class_instance,attr)):
            dict_output[attr] = getattr(class_instance, attr)
            #print(attr, ' : ' , getattr(class_instance, attr))


    # np.array / pd.Series cannot be written via json.dump ==>
    #   replace them with lists
    list_keys_replace = []
    for key, item in dict_output.items():
        if isinstance(item, np.ndarray):
            list_keys_replace.append(key)
        if isinstance(item, pd.Series):
            list_keys_replace.append(key)

    for key in list_keys_replace:
        dict_output[key] = dict_output[key].tolist()

    return dict_output

###########################################################################
#              WRITE ALL ATTRIBUTES OF CLASS INSTANCE                     #
###########################################################################
def write_LogInfo_ClassAttributes (filename, dict_LogInfo):
    """
    write dictionary with LogInformation into json file
    """
    script_dir = os.path.dirname(__file__)
    with open(os.path.join(script_dir, filename), 'w') as outfile:
        json.dump(dict_LogInfo, outfile, indent=4)

###########################################################################
#              ORDER OF COLUMNS FOR REO.csv                               #
###########################################################################
def get_REO_columnOrder ():
    """
    Return a list with the column order for REO.csv
    """

    column_order = [ 'id' ,
                     'MRM',
                     'CRM',
                     'VAR',
                     'VEV',
                     'HistoricalVEV',
                     'SRI',
                     'InvestmentAmount',
                     'InvestmentCurrency',
                     'EquivalentAmount',
                     'ScalingFactor',
                     'Pricing_Method',
                     'CarriedInterest_IHP1',
                     'CarriedInterest_IHP2',
                     'CarriedInterest_RHP',
                     'EntryCostAcquired_IHP1',
                     'EntryCostAcquired_IHP2',
                     'EntryCostAcquired_RHP',
                     'Entry_IHP1',
                     'Entry_IHP2',
                     'Entry_RHP',
                     'ExitCostAt1Year_IHP1',
                     'ExitCostAt1Year_IHP2',
                     'ExitCostAt1Year_RHP',
                     'ExitCostAtHalfRHP_IHP1',
                     'ExitCostAtHalfRHP_IHP2',
                     'ExitCostAtHalfRHP_RHP',
                     'ExitCostAtRHP_IHP1',
                     'ExitCostAtRHP_IHP2',
                     'ExitCostAtRHP_RHP',
                     'Exit_IHP1',
                     'Exit_IHP2',
                     'Exit_RHP',
                     'Other_IHP1',
                     'Other_IHP2',
                     'Other_RHP',
                     'PerformanceFees_IHP1',
                     'PerformanceFees_IHP2',
                     'PerformanceFees_RHP',
                     'Transaction_IHP1',
                     'Transaction_IHP2',
                     'Transaction_RHP',
                     'TotalCostsIHP1',
                     'TotalCostsIHP2',
                     'TotalCostsRHP',
                     'IHP1Date',
                     'IHP1Offset',
                     'IHP2Date',
                     'IHP2Offset',
                     'RHPDate',
                     'RHPOffset',
                     'RiskEngineOuput_PSFavourable_ScenarioIHP1',
                     'RiskEngineOuput_PSFavourable_ScenarioIHP2',
                     'RiskEngineOuput_PSFavourable_ScenarioRHP',
                     'RiskEngineOuput_PSModerate_ScenarioIHP1',
                     'RiskEngineOuput_PSModerate_ScenarioIHP2',
                     'RiskEngineOuput_PSModerate_ScenarioRHP',
                     'RiskEngineOuput_PSUnfavourable_ScenarioIHP1',
                     'RiskEngineOuput_PSUnfavourable_ScenarioIHP2',
                     'RiskEngineOuput_PSUnfavourable_ScenarioRHP',
                     'RiskEngineOuput_PSStressed_ScenarioIHP1',
                     'RiskEngineOuput_PSStressed_ScenarioIHP2',
                     'RiskEngineOuput_PSStressed_ScenarioRHP',
                     'NetPerformanceAmount_Favourable_ScenarioIHP1',
                     'NetPerformanceAmount_Favourable_ScenarioIHP2',
                     'NetPerformanceAmount_Favourable_ScenarioRHP',
                     'NetPerformanceAmount_Moderate_ScenarioIHP1',
                     'NetPerformanceAmount_Moderate_ScenarioIHP2',
                     'NetPerformanceAmount_Moderate_ScenarioRHP',
                     'NetPerformanceAmount_Unfavourable_ScenarioIHP1',
                     'NetPerformanceAmount_Unfavourable_ScenarioIHP2',
                     'NetPerformanceAmount_Unfavourable_ScenarioRHP',
                     'NetPerformanceAmount_Stressed_ScenarioIHP1',
                     'NetPerformanceAmount_Stressed_ScenarioIHP2',
                     'NetPerformanceAmount_Stressed_ScenarioRHP',
                     'NetPerformanceReturn_Favourable_ScenarioIHP1',
                     'NetPerformanceReturn_Favourable_ScenarioIHP2',
                     'NetPerformanceReturn_Favourable_ScenarioRHP',
                     'NetPerformanceReturn_Moderate_ScenarioIHP1',
                     'NetPerformanceReturn_Moderate_ScenarioIHP2',
                     'NetPerformanceReturn_Moderate_ScenarioRHP',
                     'NetPerformanceReturn_Unfavourable_ScenarioIHP1',
                     'NetPerformanceReturn_Unfavourable_ScenarioIHP2',
                     'NetPerformanceReturn_Unfavourable_ScenarioRHP',
                     'NetPerformanceReturn_Stressed_ScenarioIHP1',
                     'NetPerformanceReturn_Stressed_ScenarioIHP2',
                     'NetPerformanceReturn_Stressed_ScenarioRHP',
                     'RIYCarriedInterest',
                     'RIYEntryCostAcquired',
                     'RIYEntry',
                     'RIYExitCostAt1Year',
                     'RIYExitCostAtHalfRHP',
                     'RIYExitCostAtRHP',
                     'RIYExit',
                     'RIYOther',
                     'RIYPerformanceFees',
                     'RIYTransaction',
                     'RIYIHP1',
                     'RIYIHP2',
                     'RIYRHP',
                     'CF_Moment_M0',
                     'CF_Moment_M1',
                     'CF_Moment_M2',
                     'CF_Moment_M3',
                     'CF_Moment_M4',
                     'CF_Stats_Skew ',
                     'CF_Stats_Volatility',
                     'CF_Stats_Kurtosis',
                     'Stressed_Volatility_IHP1',
                     'Stressed_Volatility_IHP2',
                     'Stressed_Volatility_RHP',
                     'Stressed_Z_Alpha_IHP1',
                     'Stressed_Z_Alpha_IHP2',
                     'Stressed_Z_Alpha_RHP',
                   ]

    return column_order

def write_AttributesFile(list_Attributes, filename = "Attributes.txt", ):
    """
    write Attributes file line by line.

    Input : list of dictionaries (list_DataToWrite), 1 dict for each product.
    Return: Nothing
    """
    script_dir = os.path.dirname(__file__)
    with open(os.path.join(script_dir, filename), "w") as f_Attributes:
        for dict_product in list_Attributes:
            for key, item in dict_product.items():

                # the only item that is not written into its own line is the
                # string that goes into the beginning of each and every line
                if key == "LineStart":
                    continue

                str_NewLine = dict_product["LineStart"] + "|" + \
                              key                       + "|" + \
                              item                      + "\n"
                f_Attributes.write(str_NewLine)
