import blpapi
import pandas as pd
import datetime as dt
from pandas.tseries.offsets import *
from holidays_jp import CountryHolidays

'''Session Options + Globals'''
options = blpapi.SessionOptions()
options.setServerHost('localhost')
options.setServerPort(8194)

TIME = blpapi.Name("time")

'''Historical Data Globals'''
SECURITY_DATA = blpapi.Name("securityData")
DATE = blpapi.Name("date")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
START_DT = blpapi.Name("startDate")
END_DT = blpapi.Name("endDate")
PERIODICITY = blpapi.Name("periodicitySelection")
SECURITY_DES = blpapi.Name("Security Description")

'''Intraday Tick Data Globals'''
TICK_DATA = blpapi.Name("tickData")
COND_CODE = blpapi.Name("conditionCodes")
TICK_SIZE = blpapi.Name("size")
TYPE = blpapi.Name("type")
VALUE = blpapi.Name("value")
RESPONSE_ERROR = blpapi.Name("responseError")
CATEGORY = blpapi.Name("category")
MESSAGE = blpapi.Name("message")
SESSION_TERMINATED = blpapi.Name("SessionTerminated")

'''Intraday Bars Data Globals'''
BAR_DATA = blpapi.Name("barData")
BAR_TICK_DATA = blpapi.Name("barTickData")
OPEN = blpapi.Name("open")
HIGH = blpapi.Name("high")
LOW = blpapi.Name("low")
CLOSE = blpapi.Name("close")
VOLUME = blpapi.Name("volume")
VALUE = blpapi.Name("value")
NUM_EVENTS = blpapi.Name("numEvents")

'''Chain Globals'''
FIELD_ID = blpapi.Name("fieldId")

UTC_OFFSET = dt.datetime.utcnow() - dt.datetime.now()

def get_Hist (sec_list, fld_list, start_date, end_date):
    global options
    session = blpapi.Session(options)
    session.start()
    session.openService('//blp/refdata')
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("HistoricalDataRequest")
    for s in sec_list:
        request.append("securities",s)
    for f in fld_list:
        request.append("fields",f)
    request.set("startDate", start_date)
    request.set("endDate", end_date)
    request.set("periodicitySelection", "DAILY");
    session.sendRequest(request)
    try:
        response = {}
        # Process received events
        while(True):
            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:                    
                    securityData = msg.getElement(SECURITY_DATA)
                    secName = securityData.getElementAsString(SECURITY)
                    
                    fieldDataArray = securityData.getElement(FIELD_DATA)
                    response[secName] = {}
                    for num in range(fieldDataArray.numValues()):
                        tradedate = fieldDataArray.getValue(num).getElement(DATE).getValue()
                        close_px = fieldDataArray.getValue(num).getElement("PX_LAST").getValue()
                        response[secName][tradedate] = close_px
                                      
            # Response completely received, so we could exit
            
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
    finally:
        # Stop the session
        session.stop()
        
    tempdict = {}
    for r in response:
        tempdict[r] = pd.Series(response[r])
        
    data = pd.DataFrame(tempdict)
    return data
    

def get_Ticks(s, event_list, sdtime, edtime):
    global options 
    session = blpapi.Session(options)
    session.start()
    session.openService('//blp/refdata')
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("IntradayTickRequest")
    
    #for s in sec_list:
    request.set("security", s)
    for ev in event_list:
        request.append("eventTypes",ev)  
    
    # Convert DateTimeString to UTC datetime object
    fmt = "%Y-%m-%d" + 'T' + "%H:%M:%S"  #Assumes no milliseconds
    startDateTime = dt.datetime.strptime(sdtime, fmt) + UTC_OFFSET
    endDateTime = dt.datetime.strptime(edtime, fmt) + UTC_OFFSET
        
    request.set("startDateTime", startDateTime)
    request.set("endDateTime", endDateTime)

    #print "Sending Request:", request
    session.sendRequest(request)
    
    try:
        # Process received events
        while(True):
            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            indexlist = []
            typelist = []
            pricelist = []
            sizelist = []             
            for msg in ev:
                if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:                    
                   
                    data = msg.getElement(TICK_DATA).getElement(TICK_DATA)
                    secName = s
                    for item in data.values():
                        time = item.getElementAsDatetime(TIME)
                        time = time - UTC_OFFSET
                        timeString = item.getElementAsString(TIME)
                        type = item.getElementAsString(TYPE)
                        value = item.getElementAsFloat(VALUE)
                        size = item.getElementAsInteger(TICK_SIZE)
                        
                        indexlist.append(timeString)
                        typelist.append(type)
                        pricelist.append(value)
                        sizelist.append(size)
                        
            # Response completely received, so we could exit
            
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
    finally:
        # Stop the session
        session.stop()
    
    output = pd.DataFrame({'size': sizelist,
                               'price': pricelist,
                               'type': typelist, }, index=indexlist)
    #output = pd.DataFrame({s[:4]: pricelist}, index=indexlist)
    
    return output
    
def get_Bars(sec, event_list, sdtime, edtime, barinterval, fld_list={}):
    global options  
    session = blpapi.Session(options)
    session.start()
    session.openService('//blp/refdata')
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("IntradayBarRequest")  
    request.set("security", sec)       
    
    for e in event_list:
        request.set("eventType", e)  
    
    # Convert DateTimeString to UTC datetime object
    fmt = "%Y-%m-%d" + 'T' + "%H:%M:%S"  #Assumes no milliseconds
    startDateTime = dt.datetime.strptime(sdtime, fmt) + UTC_OFFSET
    endDateTime = dt.datetime.strptime(edtime, fmt) + UTC_OFFSET
        
    request.set("startDateTime", startDateTime)
    request.set("endDateTime", endDateTime)
    request.set("interval", barinterval) 

    #print "Sending Request:", request
    session.sendRequest(request)    
    
    try:
        # Process received events
        while(True):
            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            indexlist = []
            openlist = []
            highlist = []
            lowlist = []
            closelist = []
            numEventslist = []
            volumelist = []
            valuelist = []           
            
            for msg in ev:
                if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:                       
                    data = msg.getElement(BAR_DATA).getElement(BAR_TICK_DATA)
                    for bar in data.values():
                        time = bar.getElementAsDatetime(TIME)
                        time = time - UTC_OFFSET
                        timeString = bar.getElementAsString(TIME)
                        open = bar.getElementAsFloat(OPEN)
                        high = bar.getElementAsFloat(HIGH)
                        low = bar.getElementAsFloat(LOW)
                        close = bar.getElementAsFloat(CLOSE)
                        numEvents = bar.getElementAsInteger(NUM_EVENTS)
                        volume = bar.getElementAsInteger(VOLUME)
                        value = bar.getElementAsFloat(VALUE)
                        
                        indexlist.append(time)
                        openlist.append(open)
                        highlist.append(high)
                        lowlist.append(low)
                        closelist.append(close)
                        numEventslist.append(numEvents)
                        volumelist.append(volume)
                        valuelist.append(value)
            # Response completely received, so we could exit
            
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
    finally:
        # Stop the session
        session.stop()
        
    output = pd.DataFrame()
    
    df = pd.DataFrame({'OPEN' : openlist, \
                           'HIGH' : highlist, \
                           'LOW' : lowlist, \
                           'CLOSE' : closelist, \
                           'numEvents' : numEventslist, \
                           'VOLUME' : volumelist, \
                           'VALUE' : valuelist}, index=indexlist)      
    if not fld_list:
        output = df
    else:                       
        for f in fld_list:
            output=output.join(df[f],how="outer")
    
    return output


def get_index(index): 
    global options
    session = blpapi.Session(options)
    session.start()
    session.openService('//blp/refdata')
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")
    request.append("securities",index)
    request.append("fields","Indx_Members")  
    session.sendRequest(request)
    
    try:
        # Process received events
        response = []   
        while(True):
            ev = session.nextEvent()
            if ev.eventType() == blpapi.Event.PARTIAL_RESPONSE or ev.eventType() == blpapi.Event.RESPONSE:            
                
                for msg in ev:
                    securityDataArray = msg.getElement(SECURITY_DATA)
                    for i in range(0,securityDataArray.numValues()):
                        
                        #print the security name
                        securityData = securityDataArray.getValue(i)
                        #print securityData.getElement(SECURITY).getValue()
                        
                        #Each security element has a fieldData element with the fields requested
                        fieldData = securityData.getElement(FIELD_DATA)  
                        
                        for field in fieldData.elements():
                            if not field.isArray():
                                response = field.getValue()
                            
                            #bulk fields are returned as array
                            elif field.isArray():                           
                                for i, row in enumerate(field.values()):
                                    if i != 0:
                                        for col in field.getValue(i).elements():                                        
                                            response.append(col.getValue() + " Equity")
                                
            # Response completely received, so we could exit
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
    finally:
        # Stop the session
        session.stop()
    return response

def bbg_volcurve(ind, event, edate, numdays, interval,fld_lst):
    sec_list = blp.get_index(ind)
    volcurves = pd.DataFrame()
    fmt = "%Y-%m-%d" + 'T' + "%H:%M:%S"  #Assumes no milliseconds
    endDateTime = dt.datetime.strptime(edate, fmt)
    bday_jp = CustomBusinessDay(holidays=zip(*CountryHolidays.get('JP', int(edate[0:4])))[0])
    startDateTime = endDateTime.replace(hour=9) - numdays*BDay()
    timedelta =  pd.date_range(startDateTime, endDateTime, freq=bday_jp).nunique()
    sdate = startDateTime.strftime(fmt)
    for stock in sec_list:
        output=get_Bars(stock, event, sdate, edate, interval, fld_lst)
        output.rename(columns={'VOLUME':stock},inplace=True)
        volcurves = volcurves.join(output,how="outer")

    #process the raw data into historical averages
    volcurves.rename(columns=lambda x: x[:4], inplace=True)
    timevect = pd.Series(volcurves.index.values)
    timeframet = timevect.to_frame()
    timeframet.columns =['date']
    timeframet.set_index(timevect,inplace="True")
    timeframet['bucket'] = timeframet['date'].apply(lambda x: dt.datetime.strftime(x, '%H:%M:%S'))
    timeframet=timeframet.join(volcurves)
    volcurvesum=timeframet.groupby(['bucket']).sum()
    adv = volcurvesum.sum()/timedelta
    volcurves = volcurvesum / volcurvesum.sum()
    volcurves = volcurves.cumsum()
    volcurves = volcurves.interpolate()
    volcurvesum = volcurvesum.interpolate()
    
    return adv, volcurvesum.fillna(method=bfill), volcurves.fillna(method=bfill)






#secu = ["1332 JP Equity",'4502 JT Equity']
#fld = ["VOLUME"]
#ind = "NKY Index"
#event_l = ["TRADE"]
#sd = "2016-04-15T09:00:00"
#ed = "2016-04-15T15:00:00"
#iv = 5

#sd1 = "2016-03-17T09:00:00"
#ed1 = "2016-04-14T15:00:00"

#tst = bbg_volcurve(ind,event_l,sd,ed,iv,fld)
#vc = bbg_volcurve(secu,event_l,sd1,ed1,iv,fld)




#foo=getBars(secu,fld,event,sd,ed,iv)

'''

#sec_list = get_index(ind)
sec_list =['6758 JP Equity','4452 JP Equity']
volcurves = pd.DataFrame()
#output = pd.DataFrame()
for stock in sec_list:
    output=getBars(stock, fld, event, sd, ed, iv)
    output.rename(columns={'VOLUME':stock},inplace=True)
    volcurves = volcurves.join(output,how="outer")
    
volcurves.rename(columns=lambda x: x[:4], inplace=True)
timevect = pd.Series(volcurves.index.values)
timeframet = timevect.to_frame()
timeframet.columns =['date']
timeframet.set_index(timevect,inplace="True")
timeframet['bucket'] = timeframet['date'].apply(lambda x: dt.datetime.strftime(x, '%H:%M:%S'))
timeframet.pop('date')
timeframet=timeframet.join(volcurves)
volcurvesum=timeframet.groupby(['bucket']).sum()
vcurve = volcurvesum / volcurvesum.sum()

#newindext = timeframet.pop('bucket')
#newindext.astype('S32')
#volcurves['bucket'] = newindext
#volcurvesumt=volcurves.groupby(['bucket']).sum()
#vcurve = volcurvesumt / volcurvesumt.sum()    
    
#secs =['6758 JP Equity','4452 JP Equity','4901 JP Equity']
#fld = ["VOLUME"]
#ind = "NKY Index"
#event = ["TRADE"]
#start = "2016-04-11T09:00:00"
#end = "2016-04-11T15:00:00"
#data = pd.DataFrame()
#for sec in secs:
#    df=get_Ticks(sec,event,start,end)
#    data=data.combine_first(df)    


secs =['6758 JP Equity','4452 JP Equity']
fld = ["VOLUME"]
ind = "NKY Index"
event = ["TRADE"]
sd = "2016-04-11T09:00:00"
ed = "2016-04-11T09:05:00"
iv = 1
dd = {}
foo = pd.DataFrame()
for sec in secs:
    foo = get_Bars(sec, event, sd, ed, iv)    
    dd[sec] = foo
goo = pd.Panel(dd)

secs =['4704 JP Equity','4452 JP Equity']
test = blp.bbg_volcurve(nky,event_l,sd1,ed1,iv,fld_l)
test.pop('3103');
'''