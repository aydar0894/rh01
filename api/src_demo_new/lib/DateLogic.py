from datetime import datetime,timedelta, date
from dateutil.relativedelta import relativedelta
import re
import holidays

# Define the weekday mnemonics to match the date.weekday function
(MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)

# Define default weekends, but allow this to be overridden at the function level
# in case someone only, for example, only has a 4-day workweek.
default_weekends=(SAT,SUN)

map_holidays={
    'Argentina'           : 'AR',
    'Australia'           : 'AU',
    'Austria'             : 'AT',
    'Belgium'             : 'BE',
    'Canada'              : 'CA',
    'Colombia'            : 'CO',
    'Czech'               : 'CZ',
    'Denmark'             : 'DK',
    'EuropeanCentralBank' : 'ECB',
    'Finland'             : 'FI',
    'France'              : 'FRA',
    'Germany'             : 'DE',
    'Hungary'             : 'HU',
    'Italy'               : 'IT',
    'Japan'               : 'JP',
    'Mexico'              : 'MX',
    'Netherlands'         : 'NL',
    'NewZealand'          : 'NZ',
    'Norway'              : 'NO',
    'Polish'              : 'PL',
    'Portugal'            : 'PT',
    'PortugalExt'         : 'PTE',
    'Slovenia'            : 'SI',
    'Slovakia'            : 'SK',
    'South Africa'        : 'ZA',
    'Spain'               : 'ES',
    'Sweden'              : 'SE',
    'Switzerland'         : 'CH',
    'UnitedKingdom'       : 'UK',
    'UnitedStates'        : 'US' }

default_holidays='England'

##############################################################################
#        PRINT HOLIDAYS IN TIME PERIOD                                       #
##############################################################################
def printHolidays(country, years=[]):
    method_to_call = getattr(holidays, country)
    result = method_to_call(years=years).items()
    
    for date,name in result:
        print(date,name)

##############################################################################
#        GET HOLIDAYS IN TIME PERIOD                                         #
##############################################################################
def getHolidays(country, years=[]):
    method_to_call = getattr(holidays, country)
    result = method_to_call(years=years).items()
    
    return result       

##############################################################################
#        GET LIST OF HOLIDAYS IN TIME PERIOD                                 #
##############################################################################
def getHolidaysBetween(start_date, end_date, COUNTRY_HOLIDAYS):
    start_date = datetime.strptime(start_date, '%d/%m/%Y')
    end_date   = datetime.strptime(end_date  , '%d/%m/%Y')
    delta      = end_date - start_date
    
    # this will give you a list containing all of the dates
    LIST_DATES=[]
    years=[]
    
    for i in range(delta.days + 1):
        d=start_date + timedelta(i)
        years.append(d.year)
        LIST_DATES.append(d.strftime('%Y-%m-%d'))
    
    # match only holidays
    LIST_HOLIDAYS=[]
    for date,name in getHolidays(COUNTRY_HOLIDAYS,years):
        if date.strftime('%Y-%m-%d') in LIST_DATES:
            # [DEBUG] print(date,name)
            LIST_HOLIDAYS.append(date.strftime('%d/%m/%Y'))
            
    return LIST_HOLIDAYS

##############################################################################
#        GET LIST OF HOLIDAYS IN TIME PERIOD                                 #
##############################################################################
def listCustomHolidays():
    custom_holidays = holidays.HolidayBase()
    custom_holidays.append({"2015-01-01": "New Year's Day"})
    return custom_holidays

##############################################################################
#        GET DIGITS FROM STRING                                              #
##############################################################################
def onlyDigits(tenor):
    """
    not used
    """
    s=[int(d) for d in re.findall(r'-?\d+', tenor)][0]
    return s

##############################################################################
#        GET TENOR FROM STRING (e.g. 2Y)                                     #
##############################################################################    
def getTenorFromString(str_tenor):
    """
    extract tenor from string, 2Y --> 2
    
    only integers are supported in timedelta functions for adding to dates
    """
    if str_tenor[-1] in ["D", "W", "M", "Y"]:
        tenor = int(str_tenor[:-1])
        return tenor
    else:
        print('[ERROR]. The time unit', str_tenor[-1], ' is not recognized.')
        print('         Should be D (days), W (weeks), M (months) or Y (years)')
        return None

##############################################################################
#        CALCULATE YEAR FRACTION                                             #
##############################################################################
def getYearFraction(start_date, end_date, BusinessDayConvention='ACTACT'):
    """
    calculate year fraction between two dates.
    
    Three conventions: ACTACT, ACT365, ACT360 for year fraction.
    
    Replicates Excel function YearFraction
    """
    
    if BusinessDayConvention == 'ACTACT':
        YearFraction = float(calendardays(start_date, end_date)                                                       /\
                             calendardays(start_date, addTenor(start_date, "1Y" , flag_excludeNonBusinessDay=False)   ))
    elif BusinessDayConvention == 'ACT365':
        YearFraction = float(calendardays(start_date, end_date)                                                       /\
                             365)
    elif BusinessDayConvention == 'ACT360':
        YearFraction = float(calendardays(start_date, end_date)                                                       /\
                             360)
    else:
        print('[ERROR]. getYearFraction() only supports ACTACT, ACT365 and ACT360 conventions.')
        return None
    
    return YearFraction

##############################################################################
#        DOES STRING CONTAIN SUBSTRING?                                      #
##############################################################################
def contains(s, other):
    """
    Returns True if string contains substring
    """
    return s.__contains__(other)

##############################################################################
#        CHECK IF DAY IS WEEKDAY OR WEEKEND                                  #
##############################################################################
def isweekend(date):
    """
    Returns True if date is Weekend
    """
    val=False
    if date.weekday() > 4:
        val=True
    return val

##############################################################################
#        GET NUMBER OF CALENDAR DAYS                                         #
##############################################################################
def calendardays(start_date, end_date):
    """
    Returns the number of days between start_date and end_date
    Replicates Excel function CALENDARDAYS
    """
    
    start_date = datetime.strptime(start_date,'%d/%m/%Y')
    end_date   = datetime.strptime(end_date,'%d/%m/%Y')
    delta      = end_date - start_date
    return delta.days

##############################################################################
#        GET NUMBER OF TRADING DAYS                                          #
##############################################################################
def networkdays(start_date, end_date, holidays=[], weekends=default_weekends):
    """
    Returns the number of working days between start_date and end_date
    INCLUSIVE of both start_date and end_date. 
    Replicates Excel function NETWORKDAYS
    """
    
    start_date = datetime.strptime(start_date,'%d/%m/%Y')
    end_date   = datetime.strptime(end_date,'%d/%m/%Y')
    
    delta_days = (end_date - start_date).days + 1
    full_weeks, extra_days = divmod(delta_days, 7)
    
    # num_workdays = how many days/week you work * total # of weeks
    num_workdays = (full_weeks + 1) * (7 - len(weekends))
    
    # subtract out any working days that fall in the 'shortened week'
    for d in range(1, 8 - extra_days):
        if (end_date + timedelta(d)).weekday() not in weekends:
             num_workdays -= 1
             
    # skip holidays that fall on weekends by substracting them from num_workdays
    for d in holidays:
        d=datetime.strptime(d,'%d/%m/%Y')
        if start_date <= d <= end_date:
            num_workdays -= 1
    return num_workdays

##############################################################################
#        GET PREVIOUS WORKDAY                                                #
##############################################################################
def prev_weekday(adate,Delta):
    """
    Returns previous working day
    """
    
    adate -= timedelta(days=Delta)
    while adate.weekday() > 4:
        adate -= timedelta(days=Delta)
    return adate

##############################################################################
#        GET NEXT WORKDAY                                                    #
##############################################################################
def next_weekday(adate, Delta):
    """
    Returns next working day
    """
    adate += timedelta(days=Delta)
    while adate.weekday() > 4:
        adate += timedelta(days=Delta)
    return adate

##############################################################################
#        GET BUSINESS DAY DELTA AWAY                                         #
##############################################################################
def getBusinessDay(cdate, Delta, COUNTRY_HOLIDAYS=default_holidays):
    """
    Checks if date is business date and returns the next if not
    """
    
    #check if Weekend
    if cdate.weekday() == 5:  #check if Saturday
        if Delta > 0:
            bdate = next_weekday(cdate,2) 
        elif Delta < 0:
            bdate = prev_weekday(cdate,2)
            
    elif cdate.weekday() == 6:  #check if Sunday
        if Delta > 0:
            bdate = next_weekday(cdate,1) 
        elif Delta < 0:
            bdate = prev_weekday(cdate,1)

    else:
        bdate = cdate
    
    LIST_HOLIDAYS=[]
    for date,name in getHolidays(COUNTRY_HOLIDAYS,[bdate.year]):
        LIST_HOLIDAYS.append(date.strftime('%d/%m/%Y'))

    
    while bdate.strftime('%d/%m/%Y') in LIST_HOLIDAYS:
        if Delta > 0:
            bdate = next_weekday(bdate,1) 
        elif Delta < 0:
            bdate = prev_weekday(bdate,1)
    
    return bdate

##############################################################################
#        ADD TENOR TO DATE                                                   #
##############################################################################
def addTenor(cdate, shift, flag_excludeNonBusinessDay=True):
    """
    add shift (e.g. 1M or 2D) to date.
    
    flag_excludeNonBusinessDay==True ==> 
        if final date falls onto a NonBusinessDay, move to next date
    """
    
    cdate = datetime.strptime(cdate,'%d/%m/%Y')
    if contains(shift,'D'):
        Delta = getTenorFromString(shift)
        ndate = cdate + relativedelta(days=Delta)
    
    elif contains(shift,'W'):
        Delta = getTenorFromString(shift)
        ndate = cdate + relativedelta(weeks=Delta)
    
    elif contains(shift,'M'):
        Delta = getTenorFromString(shift)
        ndate = cdate + relativedelta(months=Delta)
    
    elif contains(shift,'Y'):
        Delta = getTenorFromString(shift)
        ndate = cdate + relativedelta(years=Delta)
    
    # make sure that the end_date falls onto a business day
    if flag_excludeNonBusinessDay == True:
        addTenor  = getBusinessDay(ndate,Delta)
        return addTenor.strftime('%d/%m/%Y')
    
    # end_date can be business or non-business day
    else:
        return ndate.strftime('%d/%m/%Y')
        
###########################################################################
#              DATE CONVERION FOR ATTRIBUTES.TXT                          #
########################################################################### 
def convertDateFormatForAttributes (str_date): 
    """
    take as input a date string (format stored in each class: DD/MM/YYYY), 
    and convert it to the format that is required by attributes.txt (YYYY-MM-DD)
    """
    
    list_date = str_date.split("/")
        
    if (len(list_date) != 3)    or \
       (int(list_date[0]) < 1)  or \
       (int(list_date[0]) > 31) or \
       (int(list_date[1]) < 1)  or \
       (int(list_date[1]) > 12) or \
       (len(list_date[0]) != 2) or \
       (len(list_date[1]) != 2) or \
       (len(list_date[2]) != 4):
        print('[ERROR - convertDateFormatForAttributes]. The date format is wrong.')
        print('         Required: DD/MM/YYYY')
        print('         Found: ', str_date)
        return None
        
    else:
        #           YYYY                 MM                   DD
        return list_date[2] + "-" + list_date[1] + "-" + list_date[0] 

###########################################################################
#              GET TENOR INFORMATION FROM RHP_STRING                      #
########################################################################### 
def getTenorInformation(str_tenorInformation):
    """
    convert RHP_string (e.g. 6M) to tenorMultiplier (6) & tenorPeriod (M)
    
    Return value: tenorMultiplier, tenorPeriod
    """
    
    tenorPeriod = str_tenorInformation[-1]
    if tenorPeriod == "D":
        str_tenorPeriod = "Day"
    elif tenorPeriod == "W":
        str_tenorPeriod = "Week"
    elif tenorPeriod == "M":
        str_tenorPeriod = "Month"
    elif tenorPeriod == "Y":
        str_tenorPeriod = "Year"
    else:
        print('[ERROR - getTenorInformation]')
        print('         Only D, W, M and Y recognized as valid tenorPeriods')
        print('         Received: ', str_tenorInformation)
        return None, None
        
    tenorMultiplier = int(str_tenorInformation[:-1])
    
    return tenorMultiplier, str_tenorPeriod