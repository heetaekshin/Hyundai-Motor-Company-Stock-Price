# 시계열 예측 모델 생성
import sqlite3
import pandas as pd
from datetime import datetime
import numpy as np
from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense, LSTM, Dropout
import matplotlib.pyplot as plt
from tensorflow.python.keras.engine import input_layer
#from Investar import Analyser

# DB연결
conn = sqlite3.connect('HMG.db')
cur = conn.cursor()

# 자료 가져오기
Daily_Price = 'Daily_Price'

df = pd.read_sql('select * from Daily_Price', con=conn)

# 날짜 (0000-00-00 00:00)형식을 '0000-00-00'형식으로 변환
df['날짜'] = pd.to_datetime(df['날짜'])

def MinMaxScaler(data):
    """최솟값과 최댓값을 0 ~ 1 사이의 값으로 변환(계산시간 단축)"""
    numerator = data - np.min(data, 0)
    denominator = np.max(data, 0) - np.min(data, 0)
    return numerator / (denominator + 1e-7) # 0으로 나누는 error 발생을 방지하기 위해 작은값 적용

df = df.set_index('날짜') # 인덱스를 날짜로 재설정

# 데이터 준비
dfx = df[['종가', '시가', '고가', '저가', '거래량']]
dfx = MinMaxScaler(dfx)
dfy = dfx[['종가']]
x = dfx.values.tolist()
y = dfy.values.tolist()
# 데이터셋 생성
data_x = []
data_y = []
size = 10 # 10일간의 종가-고가-저가-거래량-종가 데이터를 의미함
for i in range(len(y) - size):
    _x = x[i : i + size]
    _y = y[i + size]
    data_x.append(_x)
    data_y.append(_y)

# 데이터셋 분리(훈련 70, 테스트 30)
## 훈련데이터
train_size = int(len(data_y) * 0.7)
train_x = np.array(data_x[0 : train_size])
train_y = np.array(data_y[0 : train_size])
## 테스트데이터
test_size = len(data_y) - train_size
test_x = np.array(data_x[train_size : len(data_x)])
test_y = np.array(data_y[train_size : len(data_y)])

# 모델생성: RNN(순환신경망) 적용을 통한 시계열 예측
model = Sequential()
model.add(LSTM(units=10, activation='relu', return_sequences=True, input_shape=(size, 5)))
model.add(Dropout(0.1))
model.add(LSTM(units=10, activation='relu'))
model.add(Dropout(0.1))
model.add(Dense(units=1))
model.summary()

# 모델학습
model.compile(optimizer='adam', loss='mean_squared_error')
model.fit(train_x, train_y, epochs=60, batch_size=30)

pred_y = model.predict(test_x) # 학습

# 예측결과 그래프
plt.figure()
plt.plot(test_y, color='red', label='real HMG stock price')
plt.plot(pred_y, color='blue', label='predicted HMG stock price')
plt.title('HMG stock price prediction')
plt.xlabel('time')
plt.ylabel('stock price')
plt.legend()
plt.show()

# 예측 종가 출력
print("HMG tomorrow's price: ", df.종가[-1]*pred_y[-1]/dfy.종가[-1])