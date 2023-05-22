import requests
import datetime
from bs4 import BeautifulSoup
import json
from copy import deepcopy
from collections import deque
import time
import pandas as pd
import re
import logging
import os
import traceback
import sys
import concurrent.futures
import json

class CrawlingEstatesInfo:
    
    def __init__(self,host,cur_date) -> None:
        self.host=  host
        self.cur_path =os.path.dirname(os.path.realpath(__file__))
        self.cur_date = cur_date
        header_path = os.path.join(self.cur_path,'url_header.json')
        self.default_header = {}
        with open(header_path,'r') as f:
            self.default_header = json.loads(f.read())
        
        # print(f'current date {self.cur_date}')
        self.default_header['Host'] = self.host
        self.col_type = {
            'construction_company':'object',
            'use_approve':'int64',
            'detail_addrs':'string',
            'addrs':'object',
            'apt_code':'int64',
            'no':'int64',
            'apt_name':'object',
            'estate_name':'object',
            'trade_name':'object',
            'supply_area':'int64',
            'exclusive_area':'int64',
            'direction':'object',
            'confirmYmd':'int64',
            'latitude':'float64',
            'longitude':'float64',
            'price':'int64',
            'total_floor':'int64',
            'current_floor':'string'
            }
        
    # def set_current_date(self) :
        # txtfile= os.path.join(self.cur_path,'startdate.txt')
        # self.start_date = '20000101'
        # if os.path.exists(txtfile):
            # with open(txtfile,'r') as f :
            #     self.start_date= f.read().strip()


    def record_log(self,err_type,msg) :
        log_dir= os.path.join(self.cur_path,'logs')
        os.makedirs(log_dir,exist_ok=True)
                
        logger= logging.getLogger()
        logger.setLevel(err_type)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        
        file_handler=logging.FileHandler(os.path.join(log_dir,f'{err_type}_log.log'))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        if err_type==logging.ERROR:
            logger.error(msg)
        elif err_type==logging.INFO:
            logger.info(msg)



    def get_city_total_info(self,cityname):
        init_code= '0000000000'
        
        citylist=self.get_region_info(init_code)
        cityinfo= list(filter(lambda x: x['cortarName']==cityname,citylist))
        if len(cityinfo)==0:
            print(citylist)
            return
        cityinfo= cityinfo[0]
        bfs= deque([])
        bfs.append(cityinfo)
        aptlist=[]
        while bfs :
            region_no =bfs.popleft()['cortarNo']
            citylist= self.get_region_info(region_no)
            if len(citylist)>0:
                bfs+=citylist
            else :
                aptlist+=self.get_apt_list(region_no)
            
        return aptlist

    def get_specific_region(self,cityname,gu,dong):
        init_code= '0000000000'
        namelist =[cityname,gu,dong]
        
        
        # while True :
        for name in namelist:
            citylist=self.get_region_info(init_code)
            cityinfo= list(filter(lambda x: x['cortarName']==name,citylist))
            
            init_code = cityinfo[0]['cortarNo']

        complex_list=self.get_apt_list(init_code)

        save_dir = os.path.join(self.cur_path,'../dags/testdata')
        os.makedirs(save_dir,exist_ok=True)
        
        apt_list= []
        for complex_info in complex_list:
            dicts=self.get_apt_info(complex_info['complexNo'])
            apt_name =complex_info['complexName']
            if len(dicts.keys())!=0:
                apt_detail_list=self.get_for_sale(complex_info['complexNo'],dicts)
                # apt_list+=apt_detail_list
                df=pd.DataFrame.from_dict(apt_detail_list)
                if not len(df) :
                    continue
                # df.drop_duplicates(subset=['no'],keep='last',inplace=True)
                # df.dropna(inplace=True)
                # df = df.astype(self.col_type)
                save_filename= os.path.join(save_dir,f'{self.cur_date}_{dong}_{apt_name}.csv')
                df.to_csv(save_filename,index=False)
                

                # print(f"complete_{apt_name}")
            
            
            time.sleep(30)
            

        # df=pd.DataFrame.from_dict(apt_list)
        # df.drop_duplicates(subset=['no'],keep='last',inplace=True)
        # save_filename= os.path.join(self.cur_path,'testdata',f'{self.cur_date}_{dong}.csv')
        # df.to_csv(save_filename,index=False)
        # with open('./startdate.txt','w') as f:
        #     f.write(self.cur_date)


    # 시/군/구/읍/면/동
    def get_region_info(self,code):
        url = f'https://new.land.naver.com/api/regions/list?cortarNo={code}'        
        h= deepcopy(self.default_header)
        h['Referer'] = 'https://new.land.naver.com/complexes?ms=37.3855288,126.6369023,16&a=ABYG:JGC:APT&e=RETAIL'


        r= requests.get(url,data={"sameAddressGroup":"false"},headers=h)
        city = json.loads(r.text)
        citylist=[]
        if 'regionList' in city.keys():
            citylist = city['regionList']
        return citylist

    # 단지
    def get_apt_list(self,code):
        url =f'https://new.land.naver.com/api/regions/complexes?cortarNo={code}&realEstateType=ABYG%3AJGC%3AAPT&order='
        
        h= deepcopy(self.default_header)
        h['Referer'] = 'https://new.land.naver.com/complexes?ms=37.3855288,126.6369023,16&a=ABYG:JGC:APT&e=RETAIL'
        r= requests.get(url,data={"sameAddressGroup":"false"},headers=h)
        apt = json.loads(r.text)
        complex_list = apt['complexList']

        return complex_list

    def get_apt_info(self,code):
        dicts = {}
        aptinfo ={}
        try:
            url = f'https://new.land.naver.com/api/complexes/{code}?sameAddressGroup=false'

            h= deepcopy(self.default_header)
            h['Referer'] = f'https://new.land.naver.com/complexes/{code}?ms=37.411486,126.6136355,17&a=APT:ABYG:JGC&e=RETAIL'
            r= requests.get(url,data={"sameAddressGroup":"false"},headers=h)
            aptinfo = json.loads(r.text)
            cplx_detail= aptinfo['complexDetail']

            if len(cplx_detail.keys())!=0:
                # print(aptinfo.keys())
                dicts['construction_company'] = cplx_detail['constructionCompanyName'] if 'constructionCompanyName' in cplx_detail.keys() else 'unknown'
                dicts['use_approve'] = cplx_detail['useApproveYmd']
                dicts['detail_addrs']= cplx_detail['detailAddress']
                dicts['addrs']= cplx_detail['address']
                dicts['apt_code'] = code
                
            # self.get_real_price(code,dicts)
            return dicts
        except:
            cur_func= sys._getframe().f_code.co_name
            err_msg = f'{traceback.format_exc()}_{cur_func}_{aptinfo}'
            print(err_msg)
            self.record_log(logging.ERROR,err_msg)
            return dicts
        
    def get_real_price_later(self,code,dicts):
        url = f'https://new.land.naver.com/api/complexes/{code}/prices/real?complexNo={code}&tradeType=A1&year=5&priceChartChange=true&areaNo=2&type=table'
        # url =f'https://new.land.naver.com/api/complexes/{code}/prices/real?complexNo={code}&tradeType=A1&year=5&priceChartChange=false&areaNo=0&addedRowCount=51&type=table'
        h= deepcopy(self.default_header)
        h['Referer'] = f'https://new.land.naver.com/complexes/{code}?ms=37.411486,126.6136355,17&a=APT:ABYG:JGC&e=RETAIL'
        r= requests.get(url,data={"sameAddressGroup":"false"},headers=h)
        apt_price = json.loads(r.text)

        # print(apt_price)
        datalist= []
        for data in apt_price['realPriceOnMonthList'] :
            pricelist= data['realPriceList']
            for item in pricelist:
                tmpdict= deepcopy(dicts)
                tmpdict['deal_price']= item['dealPrice']
                tmpdict['deal_date'] = item['formattedTradeYearMonth']
                tmpdict['floor'] = item['floor']
                datalist.append(tmpdict)
        
        print(datalist)

    def get_for_sale(self,code,complex_dict) :
        try:
            idx= 1
            apt_list= []
            while True:
                url = f'https://new.land.naver.com/api/articles/complex/{code}?realEstateType=APT%3AABYG%3AJGC&tradeType=&\
                    tag=%3A%3A%3A%3A%3A%3A%3A%3A&rentPriceMin=0&rentPriceMax=900000000&priceMin=0&priceMax=900000000&areaMin=0&\
                    areaMax=900000000&oldBuildYears&recentlyBuildYears&minHouseHoldCount&maxHouseHoldCount&showArticle=false&sameAddressGroup=true&\
                        minMaintenanceCost&maxMaintenanceCost&priceType=RETAIL&directions=&page={idx}&complexNo={code}&buildingNos=&areaNos=&type=list&order=dateDesc'

                h= deepcopy(self.default_header)
                h['Referer'] = f'https://new.land.naver.com/complexes/{code}?ms=37.411486,126.6136355,17&a=APT:ABYG:JGC&e=RETAIL'
                r= requests.get(url,headers=h)
                try:
                    for_sale = json.loads(r.text)
                    article_list= for_sale['articleList']
                except:
                    article_list=[]
                    # print(f'err is {traceback.format_exc()}')

                if len(article_list)==0:
                    print(f'last page_{idx-1}')
                    break

                # apt_list= []
                with concurrent.futures.ThreadPoolExecutor() as ex :
                    for i,d in enumerate(ex.map(self._iter_article,article_list)):
                        if len(d.keys())!=0:
                            tmpdict= deepcopy(complex_dict)
                            tmpdict.update(d)
                            apt_list.append(tmpdict)

                idx+=1
                if article_list[-1]['articleConfirmYmd'] < self.cur_date :
                    print(f"{article_list[-1]['articleConfirmYmd']} : Past date")
                    break
            # print(apt_list[0])
            return apt_list
            
        except:
            cur_func= sys._getframe().f_code.co_name
            err_msg = f'{traceback.format_exc()}_{cur_func}'
            print(err_msg)
            print(f'r is {r.text}')
            self.record_log(logging.ERROR,err_msg)
            return []    

    def _iter_article(self,apt):
        pat = re.compile('[0-9]+')
        tmpdict={}
        try:
            print(f"data date is {apt['articleConfirmYmd']}")
            if apt['articleConfirmYmd'] != self.cur_date:
                return tmpdict

            tmpdict['no'] = apt['articleNo']
            tmpdict['apt_name'] = apt['articleName']
            # tmpdict['estate_code'] = apt['realEstateTypeCode']
            tmpdict['estate_name'] = apt['realEstateTypeName']
            tmpdict['trade_name'] = apt['tradeTypeName']
            tmpdict['supply_area'] = apt['area1']
            tmpdict['exclusive_area'] = apt['area2']
            tmpdict['direction'] = apt['direction']
            tmpdict['confirmYmd'] = apt['articleConfirmYmd']
            tmpdict['latitude'] = apt['latitude']
            tmpdict['longitude'] = apt['longitude']
            p = str(apt['dealOrWarrantPrc']).split(' ')
            front=pat.match(p[0]).group()
            back='0000'
            if len(p)>1:
                back=p[1].replace(',','').zfill(4)
                
            price= int(front+back)
            tmpdict['price'] = price
            
            floor=apt['floorInfo'].split('/')
            total_floor = floor[1].strip()
            cur_floor = floor[0].strip()
            tmpdict['total_floor'] = total_floor
            tmpdict['current_floor'] = cur_floor
            return tmpdict
        except:
            cur_func= sys._getframe().f_code.co_name
            err_msg = f'{traceback.format_exc()}_{cur_func}'
            print(err_msg)
            self.record_log(logging.ERROR,err_msg)
            return tmpdict




if __name__=='__main__':
    dt_now = datetime.datetime.now().strftime('%Y%m%d')
    # dt_now = '20230511'
    c = CrawlingEstatesInfo('new.land.naver.com',dt_now)
    st= time.time()
    # a=c.get_city_total_info("인천시")
    a= c.get_specific_region('인천시','연수구','송도동')
    # a= c.get_apt_info()
    # print(a)
    print('loading time',time.time()-st)
    
