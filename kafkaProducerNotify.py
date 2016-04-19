# pip install kafka-python
import datetime
import time
# from kafka import KafkaConsumer
from kafka import SimpleProducer, KafkaClient
import json
producerServer='192.169.34.63:9092'
producerTopic='AdServe'

consumerServer='172.29.65.53:9092'
consumerTopic='AdServe'
consumerGroup='AdServe-Streamer'

# Kafka Producer Config
kafkaProducerClient=KafkaClient(producerServer)
producer=SimpleProducer(kafkaProducerClient, async=True, req_acks=SimpleProducer.ACK_NOT_REQUIRED)

class AdClickLog(object):
    imprId="asdadasdasda"	
    clmbUserId="iproeado"
    adSltDimId="234567"
    auds="au,er"
    itemid="2000"
    algo=5
    itmClmbLId=10000
    tmpltId=321
    refUrl="google.com"
    geoDimId="144"
    clickBid=3.0
    ip="192.168.33.192"
    section="0"
    position="0"
    paid=1
    crtd=None
    advClientId=2658
    pubClientId=2310
    siteId=1
    cityDimId="10316"
    osDimId="196061"
    devTypeDimId="196047"


    # The class "constructor" - It's actually an initializer 
    def __init__(self, itmClmbLId, crtd):
        self.imprId="asdadasdasda"
        self.clmbUserId="iproeado"
        self.adSltDimId="234567"
        self.auds="au,er"
        self.itemid="2000"
        self.algo=5
        self.itmClmbLId=itmClmbLId
        self.tmpltId=321
        self.refUrl="google.com"
        self.geoDimId="144"
        self.clickBid=3.0
        self.ip="192.168.33.192"
        self.section="0"
        self.position="0"
        self.paid=1
        seconds_since_epoch=int(time.mktime(crtd.timetuple()) * 1000)
        self.crtd=seconds_since_epoch
        self.advClientId=2658
        self.pubClientId=2310
        self.siteId=1 
        self.cityDimId="10316"
        self.osDimId="196061"
        self.devTypeDimId="196047"   

        def make_adlog(itmClmbLId, crtd):    
            adlog=AdClickLog(itmClmbLId, crtd)
            return adlog

class AdImprLog(object):
    imprId="asdadasdasda"    
    clmbUserId="iproeado"
    adSltDimId="234567"
    auds="au,er,rj"
    itemid=[2000]
    algo=[5]
    itmClmbLIds=10000
    tmpltId=321
    refUrl="google.com"
    geoDimId=144
    clickBid=3
    ip="192.168.33.192"
    section="0"
    position="0"
    paid=[1]
    crtd=None
    advClientIds=[2658]
    pubClientId=2310
    siteId=1
    spend=[0.0]
    cpa=[0.0]
    pv=1
    visible=True
    expctdPayout=0.1
    cityDimId="10316"
    osDimId="196063"
    devTypeDimId="196047"
    ctxIds=[]
    meItmIds=[0]
    

    # The class "constructor" - It's actually an initializer 
    def __init__(self, itmClmbLId,  crtd):
        self.imprId="asdadasdasda"
        self.spend=[0.0]
        self.cpa=[0.0]
        self.clmbUserId="iproeado"
        self.adSltDimId="234567"
        self.position="au,er,rj"
        self.itmIds=["2000"]
        self.algo=[5]
        self.itmClmbLIds=itmClmbLId
        self.tmpltId=321
        self.refUrl="google.com"
        self.geoDimId=144
        self.clickBid=3
        self.ip="192.168.33.192"
        self.section="0"
        self.position="0"
        self.paid=[1]
        seconds_since_epoch=int(time.mktime(crtd.timetuple()) * 1000)
        self.crtd=seconds_since_epoch
        self.advClientIds=[2658]
        self.pubClientId=2310
        self.siteId=1
        self.pv=1
        self.visible=False
        self.expctdPayout   =0.1
        self.cityDimId="10316"
        self.osDimId="196063"
        self.devTypeDimId="196047"
        self.ctxIds=[[]]
        self.meItmIds=[0]

        def make_adlog(itmClmbLId, crtd):
            adlog=AdImprLog(itmClmbLId, crtd)        
            return adlog

class AdNotifyLog(object):
    imprId="asdadasdasda"
    adSltDimId="234567"
    crtd=None
    pv=0

    def __init__(self, crtd):
        self.imprId="impression produced test for pv=0 and visibility off again2"
        self.adSltDimId="1698" 	 	
        self.pv=1
        seconds_since_epoch=int(time.mktime(crtd.timetuple()) * 1000)
        self.crtd=seconds_since_epoch

        def make_adlog(crtd):
            adlog=AdNotifyLog(crtd)
            return adlog

class AdLog(object):
    adLogType=0
    adImprLog=None
    adClickLog=None
    adNotifyLog=None

    # The class "constructor" - It's actually an initializer 
    def __init__(self, adLogType, adImprLog, adClickLog, adNotifyLog):
        self.adLogType=adLogType
        self.adImprLog=adImprLog
        self.adClickLog=adClickLog
        self.adNotifyLog=adNotifyLog

        def make_adlog(adLogType, adImprLog, adClickLog, adNotifyLog):
            adlog=AdLog(adImprLog, adClickLog, adNotifyLog)
            return adlog

clickcid=10000
click=AdClickLog(clickcid, datetime.datetime.now())
cid=[10000]
impr=AdImprLog(cid, datetime.datetime.now())
notify=AdNotifyLog(datetime.datetime.now())
obj=AdLog(3, None, None, notify)
objString=json.dumps(obj, default=lambda o: o.__dict__)
# objString=json.dump(obj, default=json_util.default)
print objString
producer.send_messages(producerTopic, objString)



