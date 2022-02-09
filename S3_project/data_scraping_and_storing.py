import pandas as pd
import time
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

# selenium, BeautifulSoup를 활용한 주식데이터 수집함수 선언
def get_stock_data_during_5years_from_yahoo_finance(code):
    daily_price = [] # 가격 리스트
    col_name = ['date','open','high','low','close','adj_close','volume'] # 칼럼명 선언
    # 셀레니움을 통한 동적 스크레이핑
    options = Options()
    options.add_argument("start_maximized")
    # 야후 파이낸스 접속
    driver = webdriver.Chrome(chrome_options=options)
    yfn = 'https://finance.yahoo.com/'
    driver.get(yfn)
    # 야후 파이낸스 검색창에 code입력 및 검색
    elem = driver.find_element_by_name('yfin-usr-qry')
    elem.send_keys(code)
    elem.send_keys(Keys.RETURN)

    # Historical Data 접근
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="quote-nav"]/ul/li[5]/a'))).click()
    # 5년간 주가 불러오기
    ## Time Period 지정
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[1]/div[1]/div[1]/div/div/div'))).click()
    ## 5Y 선택
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="dropdown-menu"]/div/ul[2]/li[3]/button'))).click()
    ## Apply 버튼 클릭
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[1]/div[1]/button'))).click()

    body = driver.find_element_by_tag_name('body')
    save_len = 0
    while True:
        for i in range(100): # 현재 페이지만 크롤링하기 때문에, 5년간 데이터를 불러오기 위해 페이지 맨 아래까지 이동하여 5년 주가 데이터 loading
            body.send_keys(Keys.PAGE_DOWN)
        
        time.sleep(1)
        html = driver.page_source
        if save_len != len(html):
            save_len = len(html)
        else:
            break
    # BeautifulSoup를 통해 table 내부 값 크롤링
    html2 = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    soup = soup.find('table',{'data-test':'historical-prices'})
    price = soup.find_all('tr')[1:]

    for r in price:
        d = []
        data = r.find_all('span')

        if len(data) <= 2 or data[0].text == '*Close price adjusted for splits.': # 표 이외의 데이터 수집 방지
            continue

        dt = data[0].text
        dt = datetime.strptime(dt, '%b %d, %Y').strftime('%Y-%m-%d') # 날짜형식 조정
        d.append(dt)
        d.append(data[1].text.replace('\u202f','').replace(',','')) # 'open', 공백, ',' 제거
        d.append(data[2].text.replace('\u202f','').replace(',','')) # 'high', 공백, ',' 제거
        d.append(data[3].text.replace('\u202f','').replace(',','')) # 'low', 공백, ',' 제거
        d.append(data[4].text.replace('\u202f','').replace(',','')) # 'close', 공백, ',' 제거
        d.append(data[5].text.replace('\u202f','').replace(',','')) # 'adj_close', 공백, ',' 제거

        try:
            d.append(data[6].text.replace(',','')) # 'volume', ',' 제거
        except:
            d.append(0)

        daily_price.append(d)
    df = pd.DataFrame(daily_price, columns=col_name)
    
    driver.close()

    return df

if __name__ == '__main__':
    lmt = get_stock_data_during_5years_from_yahoo_finance('LMT')
    # csv 저장
    lmt.to_csv(path_or_buf='lmt.csv', encoding='utf-8-sig', index=False)
    # db 저장
    conn = sqlite3.connect('stock_price.db')
    cur = conn.cursor()
    lmt.to_sql('LMT', conn, index=False)