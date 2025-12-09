import yfinance as yf
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mplfinance as mpf
import numpy as np
import talib
from datetime import datetime,timedelta
import copy
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Iterable, Tuple

# Load the CSV file
#file_path = '/mnt/data/2330.csv'
#data = pd.read_csv(file_path)

# Display the first few rows of the dataset to understand its structure
#data.head()
# Convert Date column to datetime
#data['Date'] = pd.to_datetime(data['Date'])

# Display the identified W-bottoms and M-tops
#w_bottoms, m_tops
class VV_data():
    def __init__(self):
        self.Rsi5=0
        self.OC_value=0
        self.Post_value=0
        self.Date=''
        self.tsi=0
        self.Vol5=0
        self.stock=""
        self.Sma5=""
        self.Sma10=""
        self.Sma20=""
        self.Close=""
        self.Buyin=""
        self.Sellout=""
        self.Gain=""

class collectdata():
    def __init__(self, txt="Crypto.txt", strategy=" ", d_entry=False, A_period=" ", B_period=" ", exit_mode="",ma_num=20,percent=0.02,gap_week=0):
        self.address_dir="C:/Users/enoke/Desktop/MRAM/AIW20241126/AIW"
        self.DK_folder="C:/Users/enoke/Desktop/MRAM/AIW20241126/AIW/data_S/DK"
        self.WK_folder="C:/Users/enoke/Desktop/MRAM/AIW20241126/AIW/data_S/WK"
        self.MK_folder="C:/Users/enoke/Desktop/MRAM/AIW20241126/AIW/data_S/MK"
        self.TWII_wk_csv="C:/Users/enoke/Desktop/MRAM/AIW20241126/AIW/data_S/WK/^TWII.csv"
        self.txt=txt
        self.strategy=strategy
        self.d_entry=d_entry
        self.A_period=A_period
        self.B_period=B_period
        self.exit_mode=exit_mode
        self.ma_num=ma_num
        self.percent=percent
        self.gap_week=gap_week
        self.stock_id_data=self.address_dir+"/"+"stock_data"+"/"+self.txt
        #self.stock_id_data="C:/Users/HFLAB/Desktop/MRAM/AIW20241126/AIW/ETF50.txt"
        self.stock_id=[]
        #Test para
        self.teststock=""
        self.testtime=""
        #取得股票ID
        if self.teststock=="":
            address=open(self.stock_id_data,"r")
            for line in address:
                self.stock_id.append(str(line.split('\n')[0]))
            address.close()
        else:
            self.stock_id.append(self.teststock)
    def Get_data(self):       
        if not os.path.exists(self.address_dir+"/data_S"):
            os.mkdir(self.address_dir+"/data_S")
        if not os.path.exists(self.address_dir+"/data_S/Temp"):
            os.mkdir(self.address_dir+"/data_S/Temp")
        if not os.path.exists(self.address_dir+"/data_S/DK"):
            os.mkdir(self.address_dir+"/data_S/DK")
        for i in range(len(self.stock_id)):
            if(os.path.isfile(self.address_dir+"data_S/DK"+"/"+self.stock_id[i]+".csv")):
                self.df_old = pd.read_csv(self.address_dir+"data_S/DK"+"/"+self.stock_id[i]+".csv")
                last_date=self.df_old['Date'][len(self.df_old['Date'])-1]
                last_datetime=datetime.strptime(last_date,'%Y-%m-%d %H:%M:%S%z')
                #last_datetime_Day=last_datetime.strftime('%Y-%m-%d')
                last_datetime_weekday=last_datetime.weekday()
                if(last_datetime_weekday==6):
                    self.start_time=datetime.strftime((last_datetime+timedelta(days=2)),'%Y-%m-%d %H:%M:%S%z')
                elif(last_datetime_weekday==5):
                    self.start_time=datetime.strftime((last_datetime+timedelta(days=3)),'%Y-%m-%d %H:%M:%S%z')
                else:
                    self.start_time=datetime.strftime((last_datetime+timedelta(days=1)),'%Y-%m-%d %H:%M:%S%z')
                #將start_time從"0000-00-00"轉為"0000-00-00 00:00:00"
                input_format = "%Y-%m-%d %H:%M:%S%z"
                # 解析输入字符串为日期时间对象
                dt = datetime.strptime(self.start_time, input_format)
                # 格式化日期时间对象为所需的输出字符串
                output_format = "%Y-%m-%d"
                self.start_time = dt.strftime(output_format)
            else:
                self.start_time="2018-01-15"           
            self.end_time=datetime.strftime(datetime.today(),'%Y-%m-%d')
            #contract = self.api.Contracts.Stocks[self.stock_id[i]]
            #1.確定有無前資料2.從最後一天開始取資料
            #kbars = self.api.kbars(self.api.Contracts.Stocks[self.stock_id[i]], start=self.start_time, end=self.end_time)
            #df = pd.DataFrame({**kbars})
            #df.ts = pd.to_datetime(df.ts)
            #df.index = pd.to_datetime(df.ts)
            #建立DK
            #df_DK=copy.deepcopy(df)
            #df_DK.ts = df_DK.ts.dt.date
            #df_DK.index = pd.to_datetime(df_DK.ts)
            #fk_df_DK = df_DK.resample('D',label='right',closed='right').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
            # 設定股票代號
            #symbol = '2330.TW'  # 以台積電(2330)為例
            #^TWII：台灣加權股價指數（TAIEX / TSEC Weighted Index）。
            #GBPUSD=X：英鎊/美元 即期匯率（1 GBP 等於多少 USD）。
            #DX-Y.NYB：ICE 美元指數（DX）連續合約／近月報價，常見別名是 DXY（期貨報價與現貨指數會有微差）。
            #USDCAD=X：美元/加幣 即期匯率（1 USD 等於多少 CAD）。
            #USDJPY=X：美元/日圓 即期匯率（1 USD 等於多少 JPY）。
            #EURUSD=X：歐元/美元 即期匯率（1 EUR 等於多少 USD）。
            #GC=F：COMEX 黃金期貨（美元/每金衡盎司）。
            #CL=F：NYMEX 西德州原油 WTI 期貨（美元/每桶）。
            #BZ=F：ICE 布蘭特原油 Brent 期貨（美元/每桶）。
            #ES=F：標普。
            #NQ=F：那斯達克。
            #YM=F：道瓊。
            #NIY=F：小日經。
            # 使用yfinance套件抓取台股資料
            forex_symbols = {
                "^TWII",
                "GBPUSD=X",
                "DX-Y.NYB",
                "USDCAD=X",
                "USDJPY=X",
                "EURUSD=X",
                "GC=F",
                "CL=F",
                "BZ=F",
                "ES=F",
                "NQ=F",
                "YM=F",
                "NIY=F",
                "BTC-USD",
                "ETH-USD"
            }
            symbol = self.stock_id[i]
            if symbol in forex_symbols:
                stock = yf.Ticker(symbol)
            else:
                stock = yf.Ticker(symbol + ".tw")
            history = stock.history(start=self.start_time, end=self.end_time)
            fk_df_DK=copy.deepcopy(history)
            if(os.path.isfile(self.address_dir+"data_S/DK"+"/"+self.stock_id[i]+".csv")):
                fk_df_DK.to_csv('./data_S/Temp/'+self.stock_id[i]+'.csv')
                fk_df_DK=pd.read_csv('./data_S/Temp/'+self.stock_id[i]+'.csv')
                aaa=pd.concat([self.df_old['Date'],self.df_old['Open']],axis=1,join='outer')
                aaa=pd.concat([aaa,self.df_old['High']],axis=1,join='outer')
                aaa=pd.concat([aaa,self.df_old['Low']],axis=1,join='outer')
                aaa=pd.concat([aaa,self.df_old['Close']],axis=1,join='outer')
                aaa=pd.concat([aaa,self.df_old['Volume']],axis=1,join='outer')
                bbb=pd.concat([fk_df_DK['Date'],fk_df_DK['Open']],axis=1,join='outer')
                bbb=pd.concat([bbb,fk_df_DK['High']],axis=1,join='outer')
                bbb=pd.concat([bbb,fk_df_DK['Low']],axis=1,join='outer')
                bbb=pd.concat([bbb,fk_df_DK['Close']],axis=1,join='outer')
                bbb=pd.concat([bbb,fk_df_DK['Volume']],axis=1,join='outer')
                fk_df_DK=pd.concat([aaa,bbb])
            try:
                sma5 = talib.SMA(fk_df_DK['Close'],5)
                fk_df_DK["Sma5"]=sma5
                sma10 = talib.SMA(fk_df_DK['Close'],10)
                fk_df_DK["Sma10"]=sma10
                sma20 = talib.SMA(fk_df_DK['Close'],20)
                fk_df_DK["Sma20"]=sma20
                sma40 = talib.SMA(fk_df_DK['Close'],40)
                fk_df_DK["Sma40"]=sma40
                sma60 = talib.SMA(fk_df_DK['Close'],60)
                fk_df_DK["Sma60"]=sma60   
                sma80 = talib.SMA(fk_df_DK['Close'],80)
                fk_df_DK["Sma80"]=sma80                       
                sma120 = talib.SMA(fk_df_DK['Close'],120)
                fk_df_DK["Sma120"]=sma120
                sma150 = talib.SMA(fk_df_DK['Close'],150)
                fk_df_DK["Sma150"]=sma150
                vol5 = talib.SMA(fk_df_DK['Volume'],5)
                fk_df_DK["Vol5"]=vol5
                vol10 = talib.SMA(fk_df_DK['Volume'],10)
                fk_df_DK["Vol10"]=vol10
                vol20 = talib.SMA(fk_df_DK['Volume'],20)
                fk_df_DK["Vol20"]=vol20
                rsi5 = talib.RSI(fk_df_DK['Close'],5)
                fk_df_DK["Rsi5"]=rsi5
                rsi10 = talib.RSI(fk_df_DK['Close'],10)
                fk_df_DK["Rsi10"]=rsi10
                K9,D9 = talib.STOCH(fk_df_DK['High'],fk_df_DK['Low'],fk_df_DK['Close'],fastk_period=9,slowk_period=5,slowk_matype=1,slowd_period=5,slowd_matype=1)
                fk_df_DK["K9"]=K9
                fk_df_DK["D9"]=D9   
                fk_df_DK.to_csv(self.DK_folder+'/'+self.stock_id[i]+'.csv')     
                #將DK與WK分開  或以DK轉WK    
                #建立WK
                #將最後一行刪除 並接上新的資料 日期為禮拜日 將爬蟲起始往回6天                                 
            except Exception as e:
                print(f"GetData_ERROR: {self.stock_id[i]} error: {e}")
                continue

    def normalize_date_in_folder(
            self,
            input_dir: str,
            output_dir: str | None = None,
            *,
            inplace: bool = False,
            date_col: str = "Date",
            recursive: bool = False,
            save_report: bool = True,
            report_name: str = "normalize_date_report.csv",
        ):
        """
        將資料夾內所有 CSV 的 `date_col` 欄位規一化為 'YYYY-MM-DD' 字串。
        - 已是 'YYYY-MM-DD' 者保留不變
        - 其他可解析格式（含時區/時分秒）會轉為 'YYYY-MM-DD'
        - 沒有 `date_col` 的 CSV 保留原樣
        - 支援遞迴處理子資料夾與就地覆寫

        參數
        ----
        input_dir : str
            輸入資料夾路徑
        output_dir : str | None
            輸出資料夾路徑；若為 None 且非 inplace，預設為 <input_dir>/cleaned
        inplace : bool
            True 則覆寫原檔；False 則輸出到 output_dir（建議）
        date_col : str
            日期欄位名稱（預設 'Date'）
        recursive : bool
            是否遞迴處理子資料夾
        save_report : bool
            是否輸出報表 CSV
        report_name : str
            報表檔名

        回傳
        ----
        pandas.DataFrame
            每個檔案的處理結果報表（檔名、列數、轉換成功/失敗數、狀態等）
        """
        from pathlib import Path
        import pandas as pd
        import shutil
        import csv

        in_dir = Path(input_dir).expanduser().resolve()
        if not in_dir.is_dir():
            raise FileNotFoundError(f"輸入資料夾不存在：{in_dir}")

        if inplace:
            out_dir = in_dir
        else:
            out_dir = Path(output_dir).expanduser().resolve() if output_dir else (in_dir / "cleaned")
            out_dir.mkdir(parents=True, exist_ok=True)

        pattern = "**/*.csv" if recursive else "*.csv"

        def _normalize_date_series(s: pd.Series):
            """將 Series 轉為 'YYYY-MM-DD' 字串；回傳 (new_series, converted_count, failed_count)"""
            s = s.astype(str)
            mask_ok = s.str.match(r"^\d{4}-\d{2}-\d{2}$")
            to_fix = s[~mask_ok]

            # 以 UTC 解析避免時區混亂；失敗者為 NaT
            parsed = pd.to_datetime(to_fix, utc=True, errors="coerce", infer_datetime_format=True)
            # 去除時區（轉為 naive）
            try:
                parsed = parsed.dt.tz_convert(None)
            except Exception:
                pass

            converted = parsed.dt.strftime("%Y-%m-%d")
            ok_mask = parsed.notna()

            out = s.copy()
            out.loc[to_fix.index[ok_mask]] = converted[ok_mask].astype(str)

            return out, int(ok_mask.sum()), int((~ok_mask).sum())

        report_rows = []
        files = list(in_dir.glob(pattern))
        for src in files:
            rel = src.relative_to(in_dir)
            dst = src if inplace else (out_dir / rel)
            dst.parent.mkdir(parents=True, exist_ok=True)

            # 嘗試多種常見編碼（utf-8 / cp950 / big5）
            last_err = None
            df = None
            used_enc = None
            for enc in ("utf-8", "cp950", "big5"):
                try:
                    df = pd.read_csv(src, encoding=enc)
                    used_enc = enc
                    break
                except Exception as e:
                    last_err = e

            if df is None:
                # 讀取失敗：複製原檔（若非 inplace）
                if not inplace:
                    shutil.copy2(src, dst)
                report_rows.append({
                    "file": str(src),
                    "output": str(dst),
                    "status": "read_error",
                    "rows": 0,
                    "converted": 0,
                    "failed": 0,
                    "notes": str(last_err),
                })
                continue

            if date_col not in df.columns:
                # 沒有日期欄位：保留原檔
                if inplace:
                    # 不動或直接再存一次均可；這裡選擇不動
                    pass
                else:
                    shutil.copy2(src, dst)
                report_rows.append({
                    "file": str(src),
                    "output": str(dst),
                    "status": "no_date_col",
                    "rows": int(len(df)),
                    "converted": 0,
                    "failed": 0,
                    "notes": f"kept; encoding={used_enc}",
                })
                continue

            # 正規化 Date 欄位
            new_s, conv_cnt, fail_cnt = _normalize_date_series(df[date_col])
            df[date_col] = new_s

            # 輸出（預設 utf-8，失敗退回 cp950）
            out_enc = "utf-8"
            try:
                df.to_csv(dst, index=False, encoding=out_enc)
            except Exception:
                out_enc = "cp950"
                df.to_csv(dst, index=False, encoding=out_enc)

            report_rows.append({
                "file": str(src),
                "output": str(dst),
                "status": "ok",
                "rows": int(len(df)),
                "converted": int(conv_cnt),
                "failed": int(fail_cnt),
                "notes": f"read={used_enc}, write={out_enc}",
            })

        import pandas as pd
        report_df = pd.DataFrame(report_rows)
        if save_report:
            report_path = (out_dir if not inplace else in_dir) / report_name
            report_df.to_csv(report_path, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
        return report_df

    def D2W(self):
        wk_dir = os.path.join(self.address_dir, "data_S", "WK")
        os.makedirs(wk_dir, exist_ok=True)

        for stock in self.stock_id:
            try:
                # --- 讀 DK：容錯處理 Date 被當成索引或 Unnamed:0 的情況 ---
                dk_path = os.path.join(self.address_dir, "data_S", "DK", f"{stock}.csv")
                df = pd.read_csv(dk_path)

                # 若沒有 Date 欄，但第一欄是 Unnamed: 0/ index，就把它當 Date
                if 'Date' not in df.columns:
                    first_col = df.columns[0]
                    if str(first_col).lower() in ('date', '日期') or str(first_col).startswith('Unnamed'):
                        df.rename(columns={first_col: 'Date'}, inplace=True)
                    else:
                        # 最後手段：假設原本被存成索引
                        df = pd.read_csv(dk_path, index_col=0)
                        df.reset_index(inplace=True)
                        df.rename(columns={'index': 'Date'}, inplace=True)

                # 轉成 datetime，統一拿掉時區（先強制成 UTC-aware，再拿掉 tz）
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce', utc=True).dt.tz_localize(None)
                df = df.dropna(subset=['Date']).sort_values('Date').reset_index(drop=True)

                # 設為索引以做 resample
                df.set_index('Date', inplace=True)

                # --- 週K聚合 ---
                weekly_data = df.resample('W-FRI', label='right', closed='right').agg({
                    'Open': 'first',
                    'High': 'max',
                    'Low': 'min',
                    'Close': 'last',
                    'Volume': 'sum'
                })

                # 清掉無效列
                weekly_data = weekly_data.dropna(subset=['Open', 'High', 'Low', 'Close'])
                weekly_data = weekly_data[(weekly_data['Open'] != 0) &
                                        (weekly_data['High'] != 0) &
                                        (weekly_data['Low']  != 0) &
                                        (weekly_data['Close']!= 0)]

                # 尾端補K（保留你的寫法）
                if not weekly_data.empty and weekly_data.index[-1] < df.index[-1]:
                    last_period = df[df.index > weekly_data.index[-1]]
                    if not last_period.empty:
                        extra = pd.DataFrame({
                            'Open':   [last_period['Open'].iloc[0]],
                            'High':   [last_period['High'].max()],
                            'Low':    [last_period['Low'].min()],
                            'Close':  [last_period['Close'].iloc[-1]],
                            'Volume': [last_period['Volume'].sum()]
                        }, index=[last_period.index[-1]])
                        weekly_data = pd.concat([weekly_data, extra])

                # 均線 / KD / RSI （原樣）
                weekly_data['Sma5']  = weekly_data['Close'].rolling(5).mean()
                weekly_data['Sma10'] = weekly_data['Close'].rolling(10).mean()
                weekly_data['Sma20'] = weekly_data['Close'].rolling(20).mean()

                K, D = talib.STOCH(
                    weekly_data['High'].values,
                    weekly_data['Low'].values,
                    weekly_data['Close'].values,
                    fastk_period=9, slowk_period=3, slowk_matype=0,
                    slowd_period=3, slowd_matype=0
                )
                weekly_data['K9'] = K
                weekly_data['D9'] = D
                weekly_data['Rsi5']  = talib.RSI(weekly_data['Close'], 5)
                weekly_data['Rsi10'] = talib.RSI(weekly_data['Close'], 10)

                # --- 關鍵修正：確保 Date 是欄位，不是索引 ---
                weekly_data = weekly_data.copy()
                weekly_data.index = pd.to_datetime(weekly_data.index, errors='coerce')
                weekly_data.index.name = 'Date'               # 給 reset_index 正確欄名
                weekly_data = weekly_data.reset_index()       # 轉回欄位
                # 需要純年月日的話，打開下一行
                # weekly_data['Date'] = weekly_data['Date'].dt.strftime('%Y-%m-%d')

                # 一定要 index=False，避免把索引寫進檔案
                weekly_data.to_csv(os.path.join(wk_dir, f"{stock}.csv"), index=False)

            except Exception as e:
                print(f"D2W_ERROR: {stock} error: {e}")
                continue

        # 若你的 normalize_date_in_folder 會把 Date 設為索引，建議在那支函式內也統一：
        #   - 存檔時 to_csv(..., index=False)
        #   - 若 df.index.name in ('Date','date')：df.reset_index(inplace=True)
        self.normalize_date_in_folder(self.DK_folder, inplace=True, recursive=True, save_report=False)
        self.normalize_date_in_folder(self.WK_folder, inplace=True, recursive=True, save_report=False)

    def D2M(self):
        mk_dir = os.path.join(self.address_dir, "data_S", "MK")
        os.makedirs(mk_dir, exist_ok=True)
        # 讓後面 normalize 用得到（若 class 尚未有）
        if not hasattr(self, 'MK_folder'):
            self.MK_folder = mk_dir

        for stock in self.stock_id:
            try:
                # --- 讀 DK：容錯處理 Date 被當成索引或 Unnamed:0 的情況 ---
                dk_path = os.path.join(self.address_dir, "data_S", "DK", f"{stock}.csv")
                df = pd.read_csv(dk_path)

                # 若沒有 Date 欄，但第一欄是 Unnamed:0 / index，就把它當 Date
                if 'Date' not in df.columns:
                    first_col = df.columns[0]
                    if str(first_col).lower() in ('date', '日期') or str(first_col).startswith('Unnamed'):
                        df.rename(columns={first_col: 'Date'}, inplace=True)
                    else:
                        # 最後手段：假設原本被存成索引
                        df = pd.read_csv(dk_path, index_col=0)
                        df.reset_index(inplace=True)
                        df.rename(columns={'index': 'Date'}, inplace=True)

                # 轉成 datetime，統一拿掉時區（先強制成 UTC-aware，再拿掉 tz）
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce', utc=True).dt.tz_localize(None)
                df = df.dropna(subset=['Date']).sort_values('Date').reset_index(drop=True)

                # 設為索引以做 resample
                df.set_index('Date', inplace=True)

                # --- 月K聚合（月末） ---
                monthly_data = df.resample('M', label='right', closed='right').agg({
                    'Open':   'first',
                    'High':   'max',
                    'Low':    'min',
                    'Close':  'last',
                    'Volume': 'sum'
                })

                # 清掉無效列
                monthly_data = monthly_data.dropna(subset=['Open', 'High', 'Low', 'Close'])
                monthly_data = monthly_data[(monthly_data['Open']  != 0) &
                                            (monthly_data['High']  != 0) &
                                            (monthly_data['Low']   != 0) &
                                            (monthly_data['Close'] != 0)]

                # ---- 補「當月未收月」的部分 K ----
                if not df.empty:
                    if monthly_data.empty:
                        # 沒有任何完整月 → 用「目前所在月份」的所有日K做一根部分月K
                        cur_mon = df.index[-1].to_period('M')
                        last_period = df[df.index.to_period('M') == cur_mon]
                        if not last_period.empty:
                            extra = pd.DataFrame({
                                'Open':   [last_period['Open'].iloc[0]],
                                'High':   [last_period['High'].max()],
                                'Low':    [last_period['Low'].min()],
                                'Close':  [last_period['Close'].iloc[-1]],
                                'Volume': [last_period['Volume'].sum()]
                            }, index=[last_period.index[-1]])
                            monthly_data = pd.concat([monthly_data, extra])
                    else:
                        # 有完整月：若最後一筆月K早於最後一根日K所屬月份 → 補當月部分 K
                        if monthly_data.index[-1].to_period('M') < df.index[-1].to_period('M'):
                            cur_mon = df.index[-1].to_period('M')
                            last_period = df[df.index.to_period('M') == cur_mon]
                            if not last_period.empty:
                                extra = pd.DataFrame({
                                    'Open':   [last_period['Open'].iloc[0]],
                                    'High':   [last_period['High'].max()],
                                    'Low':    [last_period['Low'].min()],
                                    'Close':  [last_period['Close'].iloc[-1]],
                                    'Volume': [last_period['Volume'].sum()]
                                }, index=[last_period.index[-1]])
                                monthly_data = pd.concat([monthly_data, extra])

                # 均線 / KD / RSI（與週K版一致，月份資料計算）
                monthly_data['Sma5']  = monthly_data['Close'].rolling(5).mean()
                monthly_data['Sma10'] = monthly_data['Close'].rolling(10).mean()
                monthly_data['Sma20'] = monthly_data['Close'].rolling(20).mean()

                # talib: KD/RSI
                K, D = talib.STOCH(
                    monthly_data['High'].values,
                    monthly_data['Low'].values,
                    monthly_data['Close'].values,
                    fastk_period=9, slowk_period=3, slowk_matype=0,
                    slowd_period=3, slowd_matype=0
                )
                monthly_data['K9'] = K
                monthly_data['D9'] = D
                monthly_data['Rsi5']  = talib.RSI(monthly_data['Close'], 5)
                monthly_data['Rsi10'] = talib.RSI(monthly_data['Close'], 10)

                # --- 確保 Date 為欄位而非索引 ---
                monthly_data = monthly_data.copy()
                monthly_data.index = pd.to_datetime(monthly_data.index, errors='coerce')
                monthly_data.index.name = 'Date'
                monthly_data = monthly_data.reset_index()
                # 若你要只有年月日，打開下一行：
                # monthly_data['Date'] = monthly_data['Date'].dt.strftime('%Y-%m-%d')

                # 寫檔（一定 index=False，避免把索引寫進 CSV）
                monthly_data.to_csv(os.path.join(mk_dir, f"{stock}.csv"), index=False)

            except Exception as e:
                print(f"D2M_ERROR: {stock} error: {e}")
                continue

        # 正規化日期格式
        self.normalize_date_in_folder(self.DK_folder, inplace=True, recursive=True, save_report=False)
        # MK_folder 若不存在則用 mk_dir
        mk_folder = getattr(self, 'MK_folder', mk_dir)
        self.normalize_date_in_folder(mk_folder, inplace=True, recursive=True, save_report=False)

    def batch_backtest_sma_strategy(self, export_dir='SMA'):
        """
        批量回測資料夾內全部股票，回傳所有交易記錄DataFrame，並匯出Excel。
        """
        stockid_name = Path(self.stock_id_data).stem  # -> "ETF50"
        # 建立資料夾（如果不存在）
        export_dir=self.strategy
        os.makedirs(export_dir, exist_ok=True)
        # 新增 子資料夾
        stockid_dir = os.path.join(export_dir, stockid_name)
        os.makedirs(stockid_dir, exist_ok=True)
        export_path = os.path.join(stockid_dir, 'all_symbols_trades.xlsx')
        summary_path = os.path.join(stockid_dir, 'latest_trades_summary.xlsx')
        summurized_path = os.path.join(stockid_dir, 'SSP.xlsx')
        levels_path = os.path.join(stockid_dir, 'turn_candle_summary.xlsx')
        results = []
        levels_results = []  # 🔸新增：用來收集每個 symbol 的 levels_df
        for filename in self.stock_id:
            if filename.endswith('.csv'):
                symbol = filename[:-4]
            else :
                symbol = filename
            if not filename.lower().endswith(".csv"):
                filename += ".csv"
            daily_csv = os.path.join(self.DK_folder, filename)
            weekly_csv = os.path.join(self.WK_folder, filename)
            monthly_csv = os.path.join(self.MK_folder, filename)
            if not os.path.exists(weekly_csv):
                print(f'[警告] 找不到週線: {filename}')
                continue
            print(f'=== {symbol} 回測中 ===')
            if export_dir=="RSI":
                trades_df = collectdata.backtest_weekly_rsi_cross_long_short(weekly_csv, daily_csv, self.TWII_wk_csv, show_summary=False)
            elif export_dir=="SMA":
                trades_df = collectdata.backtest_sma_strategy_V3(weekly_csv, daily_csv, show_summary=False)
                #trades_df = collectdata.backtest_sma_strategy_V5(weekly_csv, daily_csv, signal_tf="month", ma_days=20, show_summary=False, direct_entry_no_retest=self.d_entry)
                #trades_df = collectdata.backtest_sma_strategy_V6(weekly_csv, daily_csv, signal_tf=self.A_period, retest_tf=self.B_period, ma_days=20, tp_pct=0.035, monthly_csv=monthly_csv, direct_entry_no_retest=self.d_entry)
            elif export_dir=="VA":
                #trades_df, levels_df = collectdata.backtest_candle_turn_strategy_v2(weekly_csv,daily_csv, tp_pct=0.03)
                #trades_df, levels_df = collectdata.backtest_candle_turn_strategy_v3(weekly_csv,daily_csv, tp_pct=0.03, direct_entry_no_retest=self.d_entry)
                trades_df, levels_df = collectdata.backtest_candle_turn_strategy_v6(weekly_csv,daily_csv, signal_tf=self.A_period,max_gap_weeks=self.gap_week, retest_tf=self.B_period,tp_pct=self.percent,exit_mode=self.exit_mode,exit_ma_days=self.ma_num, monthly_csv=monthly_csv, direct_entry_no_retest=self.d_entry)
            elif export_dir=="VAR":
                trades_df, levels_df =collectdata.backtest_daily_turn_at_weekly_level_v1(weekly_csv,daily_csv, tp_pct=0.03)
                # 🔸新增：收集轉折水平表
                if isinstance(levels_df, pd.DataFrame) and not levels_df.empty:
                    levels_df = levels_df.copy()
                    levels_df['symbol'] = symbol
                    levels_results.append(levels_df)
            elif export_dir=="NVA":
                #trades_df, levels_df = collectdata.backtest_candle_turn_strategy_v2(weekly_csv,daily_csv, tp_pct=0.03)
                #trades_df, levels_df = collectdata.backtest_candle_turn_strategy_v3(weekly_csv,daily_csv, tp_pct=0.03, direct_entry_no_retest=self.d_entry)
                #trades_df, levels_df = collectdata.backtest_candle_turn_strategy_v6(weekly_csv,daily_csv, signal_tf=self.A_period,max_gap_weeks=self.gap_week, retest_tf=self.B_period,tp_pct=self.percent,exit_mode=self.exit_mode,exit_ma_days=self.ma_num, monthly_csv=monthly_csv, direct_entry_no_retest=self.d_entry)
                trades_df, levels_df = collectdata.backtest_candle_turn_strategy_v7(weekly_csv,daily_csv, signal_tf=self.A_period,max_gap_weeks=self.gap_week,tp_pct=self.percent,exit_mode=self.exit_mode,exit_ma_days=self.ma_num, monthly_csv=monthly_csv)
            if not trades_df.empty:
                trades_df['symbol'] = symbol
                results.append(trades_df)

            
        # 匯總
        if results:
            all_trades = pd.concat(results, ignore_index=True)
            # 去掉時間欄位的時區（避免存檔報錯）
            all_trades = all_trades.apply(
                lambda x: x.dt.tz_localize(None) if pd.api.types.is_datetime64tz_dtype(x) else x
            )

            # 確保目錄存在
            os.makedirs(export_dir, exist_ok=True)

            # 全部交易明細
            all_trades.to_excel(export_path, index=False)

            # ===== 這裡開始新增（保留你原本 summary 輸出） =====
            # 只取每個 symbol 最新一筆（用進場日排序）
            all_trades_sorted = all_trades.sort_values(['symbol', 'entry_date'])
            latest_trades = all_trades_sorted.groupby('symbol').tail(1)

            # 只顯示想看的欄位
            latest_trades_simple = latest_trades[
                ['symbol', 'entry_date', 'entry_price', 'exit_date', 'exit_price', 'direction', 'pnl', 'pnl_pct']
            ]
            print("\n=== 每支股票最近一筆進出場資訊 ===")
            print(latest_trades_simple)

            # 另存一份 excel（summary）
            latest_trades_simple.to_excel(summary_path, index=False)
            print(f'\n已存檔：{summary_path}')
            # ====== 結束新增 ======
            # ===== 這裡開始新增：每個 symbol 統計（勝率 / 筆數 / 總獲利 等） =====
            # ===== 這裡開始（以 pnl_pct 為單位的每個 symbol 統計） =====
            _tmp = all_trades_sorted.copy()
            _tmp['pnl_pct'] = pd.to_numeric(_tmp['pnl_pct'], errors='coerce')
            _tmp = _tmp.dropna(subset=['pnl_pct'])

            if not _tmp.empty and 'symbol' in _tmp.columns:
                grp = _tmp.groupby('symbol', dropna=False)

                # 以百分比欄位彙總
                base = grp['pnl_pct'].agg(
                    trades='size',
                    total_pnl_pct_sum='sum',   # 各交易 % 直接相加（簡單加總）
                    avg_pnl_pct='mean',
                    median_pnl_pct='median',
                ).reset_index()

                # 勝場：以 pnl_pct > 0 判定
                wins = grp.apply(lambda g: (g['pnl_pct'] > 0).sum()).rename('wins').reset_index()

                # 複利總報酬%： (∏(1 + pnl_pct/100) - 1) * 100
                comp_pct = grp['pnl_pct'].apply(lambda s: ((1.0 + s/100.0).prod() - 1.0) * 100.0) \
                                        .rename('compounded_ret_pct') \
                                        .reset_index()

                # 合併與整理
                symbol_stats = base.merge(wins, on='symbol', how='left').merge(comp_pct, on='symbol', how='left')
                symbol_stats['win_rate'] = symbol_stats['wins'] / symbol_stats['trades']

                # 欄位順序 + 排序（先看複利% → 加總% → 勝率 → 筆數）
                symbol_stats = symbol_stats[
                    ['symbol','trades','wins','win_rate','compounded_ret_pct','total_pnl_pct_sum','avg_pnl_pct','median_pnl_pct']
                ].sort_values(['compounded_ret_pct','total_pnl_pct_sum','win_rate','trades'],
                            ascending=[False, False, False, False])

                # 輸出 Excel / CSV
                symbol_stats.to_excel(summurized_path, index=False)
                ssp_csv_path = os.path.join(stockid_dir, 'SSP.csv')
                symbol_stats.to_csv(ssp_csv_path, index=False, encoding='utf-8-sig')

                print(f"已存檔：{summurized_path}")
                print(f"已存檔：{ssp_csv_path}")

                print("\n=== 每個 symbol 統計（Top 10 by compounded_ret_pct）===")
                print(symbol_stats.head(10).to_string(index=False))
            else:
                print("\n[提示] 無可用的 pnl_pct 或 symbol 欄位，略過 per-symbol 統計。")
            # ===== 這裡結束 =====
            # 🔸新增：彙總 & 輸出轉折水平表
            if levels_results:
                all_levels = pd.concat(levels_results, ignore_index=True)
                all_levels = all_levels.apply(
                    lambda x: x.dt.tz_localize(None) if pd.api.types.is_datetime64tz_dtype(x) else x
                )
                #levels_path = os.path.join(export_dir, 'turn_levels.xlsx')  # 另存一檔
                all_levels.to_excel(levels_path, index=False)
                print(f'已存檔：{levels_path}')

            print(f'\n--- 完成，已輸出至 {export_path} ---')
            print('總筆數:', len(all_trades))
            print('勝率:', (all_trades["pnl"] > 0).mean())
            print('總損益:', all_trades["pnl_pct"].sum())
            return all_trades
        else:
            print('沒有符合條件的交易')
            return pd.DataFrame()

    def backtest_weekly_rsi_cross_long_short(weekly_csv, daily_csv, twii_wk_csv, show_summary=False):
        """
        週RSI5上穿RSI10做多，下穿做空
        進出場皆以交叉後「下週第一個日線開盤價」成交
        大盤濾網：多單需TWII收盤在週SMA5之上，空單需在週SMA5之下
        """
        import pandas as pd

        wk = pd.read_csv(weekly_csv, parse_dates=['Date'])
        dk = pd.read_csv(daily_csv, parse_dates=['Date'])
        twii = pd.read_csv(twii_wk_csv, parse_dates=['Date'])

        wk = wk.sort_values('Date').reset_index(drop=True)
        dk = dk.sort_values('Date').reset_index(drop=True)
        twii = twii.sort_values('Date').reset_index(drop=True)

        # RSI交叉
        wk['prev_Rsi5'] = wk['Rsi5'].shift(1)
        wk['prev_Rsi10'] = wk['Rsi10'].shift(1)
        wk['golden_cross'] = (wk['prev_Rsi5'] < wk['prev_Rsi10']) & (wk['Rsi5'] > wk['Rsi10'])
        wk['dead_cross']   = (wk['prev_Rsi5'] > wk['prev_Rsi10']) & (wk['Rsi5'] < wk['Rsi10'])

        trades = []
        position = None
        entry_row = None
        entry_type = None

        for idx, row in wk.iterrows():
            # 找下週第一個有交易的日線
            if idx + 1 < len(wk):
                next_week_start = wk.loc[idx, 'Date'] + pd.Timedelta(days=1)
                next_week_end = wk.loc[idx+1, 'Date']
                mask = (dk['Date'] >= next_week_start) & (dk['Date'] < next_week_end)
                next_week_dk = dk[mask]
            else:
                next_week_dk = dk[dk['Date'] > wk.loc[idx, 'Date']]

            if not next_week_dk.empty:
                next_open_row = next_week_dk.iloc[0]
                next_open_date = next_open_row['Date']
                next_open_price = next_open_row['Open']
            else:
                continue  # 無法找到下週開盤日，跳過

            # 找對應週的加權指數收盤和SMA5
            twii_row = twii[twii['Date'] == row['Date']]
            if twii_row.empty:
                continue  # 大盤無資料，跳過這週
            twii_close = twii_row.iloc[0]['Close']
            twii_sma5 = twii_row.iloc[0]['Sma5']

            # 多單需大盤站上週SMA5，空單需大盤跌破週SMA5
            if row['golden_cross']:
                if not (twii_close > twii_sma5):
                    continue  # 大盤未站上，不做多
                if position is None:
                    position = 'long'
                    entry_row = {'Date': next_open_date, 'Open': next_open_price}
                    entry_type = 'long'
                elif position == 'short':
                    # 空單反手平倉並做多
                    trades.append({
                        'direction': 'short',
                        'entry_date': entry_row['Date'],
                        'entry_price': entry_row['Open'],
                        'exit_date': next_open_date,
                        'exit_price': next_open_price,
                        'pnl': entry_row['Open'] - next_open_price,
                        'pnl_pct': (entry_row['Open'] - next_open_price) / entry_row['Open'] * 100
                    })
                    position = 'long'
                    entry_row = {'Date': next_open_date, 'Open': next_open_price}
                    entry_type = 'long'
                continue

            if row['dead_cross']:
                if not (twii_close < twii_sma5):
                    continue  # 大盤未跌破，不做空
                if position is None:
                    position = 'short'
                    entry_row = {'Date': next_open_date, 'Open': next_open_price}
                    entry_type = 'short'
                elif position == 'long':
                    # 多單反手平倉並做空
                    trades.append({
                        'direction': 'long',
                        'entry_date': entry_row['Date'],
                        'entry_price': entry_row['Open'],
                        'exit_date': next_open_date,
                        'exit_price': next_open_price,
                        'pnl': next_open_price - entry_row['Open'],
                        'pnl_pct': (next_open_price - entry_row['Open']) / entry_row['Open'] * 100
                    })
                    position = 'short'
                    entry_row = {'Date': next_open_date, 'Open': next_open_price}
                    entry_type = 'short'
                continue

        # 最後一筆未平倉
        if position is not None and entry_row is not None:
            last_row = dk[dk['Date'] >= entry_row['Date']].iloc[-1]
            trades.append({
                'direction': entry_type,
                'entry_date': entry_row['Date'],
                'entry_price': entry_row['Open'],
                'exit_date': last_row['Date'],
                'exit_price': last_row['Close'],
                'pnl': (last_row['Close'] - entry_row['Open']) if position == 'long' else (entry_row['Open'] - last_row['Close']),
                'pnl_pct': ((last_row['Close'] - entry_row['Open']) / entry_row['Open'] * 100) if position == 'long' else ((entry_row['Open'] - last_row['Close']) / entry_row['Open'] * 100)
            })

        trades_df = pd.DataFrame(trades)
        if not trades_df.empty:
            trades_df['holding_weeks'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days // 7
        if show_summary and not trades_df.empty:
            print("總交易次數:", len(trades_df))
            print("平均損益(%)", trades_df['pnl_pct'].mean())
            print("勝率:", (trades_df['pnl'] > 0).mean())
            print("平均持有週數:", trades_df['holding_weeks'].mean())
        return trades_df

    def backtest_sma_strategy(weekly_csv, daily_csv, show_summary=False):
        """單一股票策略回測（嚴謹：週線交叉後的「下一週」日線進場）"""
        wk = pd.read_csv(weekly_csv, parse_dates=['Date'])
        dk = pd.read_csv(daily_csv, parse_dates=['Date'])
        if 'Sma20' not in dk.columns:
            dk['Sma20'] = dk['Close'].rolling(20).mean()
        wk = wk.sort_values('Date').reset_index(drop=True)
        dk = dk.sort_values('Date').reset_index(drop=True)
        wk['prev_Close'] = wk['Close'].shift(1)
        wk['prev_Sma5'] = wk['Sma5'].shift(1)
        wk['cross'] = (
            ((wk['prev_Close'] < wk['prev_Sma5']) & (wk['Close'] > wk['Sma5'])) |
            ((wk['prev_Close'] > wk['prev_Sma5']) & (wk['Close'] < wk['Sma5']))
        )
        # 日線標記紅綠
        dk['is_red'] = dk['Close'] > dk['Open']
        dk['is_green'] = dk['Close'] < dk['Open']
        dk['prev_is_red'] = dk['is_red'].shift(1)
        dk['prev_is_green'] = dk['is_green'].shift(1)
        dk['prev_High'] = dk['High'].shift(1)
        dk['prev_Low'] = dk['Low'].shift(1)
        dk['prev_Sma20'] = dk['Sma20'].shift(1)

        trades = []
        # 以週線交叉的「下一週」區間搜尋進場
        for idx, row in wk[wk['cross']].iterrows():
            # 下一週起訖
            this_week_end = wk.loc[idx, 'Date']
            next_week_start = this_week_end + pd.Timedelta(days=1)
            next_week_end = next_week_start + pd.Timedelta(days=4)
            mask = (dk['Date'] >= next_week_start) & (dk['Date'] <= next_week_end)
            this_week_dk = dk[mask].copy()
            if this_week_dk.empty:
                continue

            prev_c, prev_s = row['prev_Close'], row['prev_Sma5']
            now_c, now_s = row['Close'], row['Sma5']
            direction = 'long' if (prev_c < prev_s and now_c > now_s) else 'short'

            if direction == 'short':
                hit = this_week_dk[
                    (this_week_dk['prev_is_red']) & (this_week_dk['is_green']) &
                    (this_week_dk['prev_High'] > this_week_dk['prev_Sma20']) &
                    (this_week_dk['Close'] < this_week_dk['Sma20'])
                ]
            elif direction == 'long':
                hit = this_week_dk[
                    (this_week_dk['prev_is_green']) & (this_week_dk['is_red']) &
                    (this_week_dk['prev_Low'] < this_week_dk['prev_Sma20']) &
                    (this_week_dk['Close'] > this_week_dk['Sma20'])
                ]
            else:
                hit = pd.DataFrame()
            if hit.empty:
                continue
            entry_row = hit.iloc[0]
            entry_date = entry_row['Date']
            entry_price = entry_row['Close']
            hold_mask = dk['Date'] > entry_date
            future = dk[hold_mask].copy()
            if future.empty:
                continue

            future['prev_Close'] = future['Close'].shift(1)
            future['prev_Sma20'] = future['Sma20'].shift(1)
            if direction == 'long':
                cond = (future['prev_Close'] >= future['prev_Sma20']) & (future['Close'] < future['Sma20'])
            else:
                cond = (future['prev_Close'] <= future['prev_Sma20']) & (future['Close'] > future['Sma20'])
            if not future[cond].empty:
                exit_row = future[cond].iloc[0]
            else:
                exit_row = future.iloc[-1]
            exit_date = exit_row['Date']
            exit_price = exit_row['Close']
            if direction == 'long':
                pnl = exit_price - entry_price
                pnl_pct = (exit_price - entry_price) / entry_price * 100
            else:
                pnl = entry_price - exit_price
                pnl_pct = (entry_price - exit_price) / entry_price * 100
            trades.append({
                'entry_date': entry_date,
                'entry_price': entry_price,
                'exit_date': exit_date,
                'exit_price': exit_price,
                'direction': direction,
                'pnl': pnl,
                'pnl_pct': pnl_pct
            })           
        trades_df = pd.DataFrame(trades)
        if not trades_df.empty:
            trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days
        return trades_df

    def backtest_sma_strategy_V2(
        weekly_csv,
        daily_csv,
        show_summary=False,
        weekly_price_col='Close',   # 週線用哪個價格欄位（預設 Close）
        weekly_ma_col='Sma10'        # 週線均線欄位（預設 CSV 中的 Sma5）
        ):
            """
            週線交叉進場；出場條件為再度發生相反方向的「週 價格(weekly_price_col) 與 週均線(weekly_ma_col) 的跨越」，
            並在觸發週的『週收盤』出場。

            進場：週線 價格 與 週均線 發生交叉（上穿做多、下穿做空）
                → 下一個有資料的交易日『日線開盤』進場
            出場（做多）：週線 價格 從在 週均線 上方 → 跌破 週均線 的當週收盤
            出場（做空）：週線 價格 從在 週均線 下方 → 突破 週均線 的當週收盤
            """
            import pandas as pd

            # 讀入並排序
            wk = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            dk = pd.read_csv(daily_csv,  parse_dates=['Date']).sort_values('Date').reset_index(drop=True)

            # 欄位檢查（週）
            for col in ['Date', weekly_price_col, weekly_ma_col]:
                if col not in wk.columns:
                    raise ValueError(f"Weekly CSV 缺少欄位：{col}")

            # 欄位檢查（日）
            for col in ['Date', 'Open']:
                if col not in dk.columns:
                    raise ValueError(f"Daily CSV 缺少欄位：{col}")

            # 準備前值（用來偵測「跨越」）
            wk['prev_price'] = wk[weekly_price_col].shift(1)
            wk['prev_ma']    = wk[weekly_ma_col].shift(1)

            # 偵測「進場用」交叉（上穿或下穿）
            wk['cross'] = (
                ((wk['prev_price'] < wk['prev_ma']) & (wk[weekly_price_col] > wk[weekly_ma_col])) |
                ((wk['prev_price'] > wk['prev_ma']) & (wk[weekly_price_col] < wk[weekly_ma_col]))
            )

            trades = []

            for idx, row in wk[wk['cross']].iterrows():
                # ── 進場：訊號週之後的第一個「有資料」的日線開盤 ───────────────────
                next_week_start = wk.loc[idx, 'Date'] + pd.Timedelta(days=1)
                mask = dk['Date'] == next_week_start
                if not mask.any():
                    future_days = dk[dk['Date'] > wk.loc[idx, 'Date']]
                    if future_days.empty:
                        continue
                    entry_row = future_days.iloc[0]
                else:
                    entry_row = dk[mask].iloc[0]

                entry_date  = entry_row['Date']
                entry_price = float(entry_row['Open'])

                # 判斷多空方向
                prev_p, prev_m = row['prev_price'], row['prev_ma']
                now_p,  now_m  = row[weekly_price_col], row[weekly_ma_col]
                direction = 'long' if (prev_p < prev_m and now_p > now_m) else 'short'

                # ── 出場：反向交叉的當週收盤 ─────────────────────────────────
                future_wk = wk[wk['Date'] > wk.loc[idx, 'Date']]
                if future_wk.empty:
                    continue

                if direction == 'long':
                    # 從 (>=) 上方 → 跌破
                    cond = (future_wk['prev_price'] >= future_wk['prev_ma']) & \
                        (future_wk[weekly_price_col] < future_wk[weekly_ma_col])
                else:
                    # 從 (<=) 下方 → 突破
                    cond = (future_wk['prev_price'] <= future_wk['prev_ma']) & \
                        (future_wk[weekly_price_col] > future_wk[weekly_ma_col])

                if not future_wk[cond].empty:
                    exit_row_wk = future_wk[cond].iloc[0]   # 第一個觸發週
                else:
                    exit_row_wk = future_wk.iloc[-1]        # 無觸發 → 最後一週強制出場

                exit_date  = exit_row_wk['Date']
                exit_price = float(exit_row_wk[weekly_price_col])  # 當週收盤（或你可改成用 Close）

                # 計算損益
                if direction == 'long':
                    pnl     = exit_price - entry_price
                    pnl_pct = (pnl / entry_price) * 100.0
                else:
                    pnl     = entry_price - exit_price
                    pnl_pct = (pnl / entry_price) * 100.0

                trades.append({
                    'entry_date':  entry_date,
                    'entry_price': entry_price,
                    'exit_date':   exit_date,
                    'exit_price':  exit_price,
                    'direction':   direction,
                    'pnl':         float(pnl),
                    'pnl_pct':     float(pnl_pct),
                })

            trades_df = pd.DataFrame(trades)
            if not trades_df.empty:
                trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days
                if show_summary:
                    n   = len(trades_df)
                    wr  = (trades_df['pnl'] > 0).mean()
                    tot = (1 + trades_df['pnl_pct']/100).prod() - 1
                    print(f"Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}")

            return trades_df

    def backtest_sma_strategy_V3(weekly_csv, daily_csv, show_summary=False):
        """
        進場條件（只看「訊號週」之後的『下一週』日線）：
        多頭：週 Close 上穿 週SMA5 -> 下一週日線出現
                (A) 日 Close 由下穿上 20MA，或
                (B) 日 Low ≤ 20MA 且 Close > Open（碰線後轉強）
                → 觸發日『下一個交易日開盤』進場做多
        空頭：週 Close 下穿 週SMA5 -> 下一週日線出現
                (A) 日 Close 由上穿下 20MA，或
                (B) 日 High ≥ 20MA 且 Close < Open（碰線後轉弱）
                → 觸發日『下一個交易日開盤』進場放空

        出場條件：
        多頭：第一次出現 週 Close 跌破 週SMA5 的那一週收盤。
        空頭：第一次出現 週 Close 突破 週SMA5 的那一週收盤。
        """
        import pandas as pd
        import numpy as np

        # 讀入並排序
        wk = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        dk = pd.read_csv(daily_csv,  parse_dates=['Date']).sort_values('Date').reset_index(drop=True)

        # 準備週線前一週欄位（偵測跨越）
        wk['prev_Close'] = wk['Close'].shift(1)
        wk['prev_Sma5']  = wk['Sma5'].shift(1)

        # 週線訊號
        wk['bull_cross'] = (wk['prev_Close'] < wk['prev_Sma5']) & (wk['Close'] > wk['Sma5'])  # 上穿
        wk['bear_cross'] = (wk['prev_Close'] > wk['prev_Sma5']) & (wk['Close'] < wk['Sma5'])  # 下穿

        # 日線計算 20 日
        dk['SMA20']      = dk['Close'].rolling(20, min_periods=20).mean()
        dk['prev_Close'] = dk['Close'].shift(1)
        dk['prev_SMA20'] = dk['SMA20'].shift(1)

        trades = []

        def _find_entry_in_next_week(signal_week_idx: int, side: str):
            """
            在『下一週』日線中尋找回測20MA的觸發點並回傳 (entry_date, entry_price, trigger_date, trigger_type) 或 None
            side: 'long' or 'short'
            """
            # 僅限『下一週』： (week_date, next_week_date]
            if signal_week_idx + 1 >= len(wk):
                return None
            week_signal_date = wk.loc[signal_week_idx, 'Date']
            next_week_end    = wk.loc[signal_week_idx + 1, 'Date']
            next_week_start  = week_signal_date  # 嚴格「下一週」→ 日線必須 > 訊號週日期

            dw = dk[(dk['Date'] > next_week_start) & (dk['Date'] <= next_week_end)].copy()
            if dw.empty:
                return None

            if side == 'long':
                # (A) 上穿20MA
                cond_cross = (dw['prev_Close'] < dw['prev_SMA20']) & (dw['Close'] > dw['SMA20'])
                # (B) 碰到20MA後轉強
                cond_touch = (dw['Low'] <= dw['SMA20']) & (dw['Close'] > dw['Open'])
            else:  # short
                # (A) 下穿20MA
                cond_cross = (dw['prev_Close'] > dw['prev_SMA20']) & (dw['Close'] < dw['SMA20'])
                # (B) 碰到20MA後轉弱
                cond_touch = (dw['High'] >= dw['SMA20']) & (dw['Close'] < dw['Open'])

            dw['retest_ok'] = cond_cross | cond_touch
            if dw[dw['retest_ok']].empty:
                return None

            trigger_row = dw[dw['retest_ok']].iloc[0]
            trigger_date = trigger_row['Date']
            trigger_type = 'cross' if cond_cross.loc[trigger_row.name] else 'touch'

            # 進場：觸發日『下一個交易日開盤』
            future_days = dk[dk['Date'] > trigger_date]
            if future_days.empty:
                return None
            entry_row = future_days.iloc[0]
            return (entry_row['Date'], float(entry_row['Open']), trigger_date, trigger_type)

        def _find_exit_after(signal_week_date: pd.Timestamp, side: str):
            """
            依週線找第一個反向跨越的出場週（含該週的 Close 價格）
            """
            future_wk = wk[wk['Date'] > signal_week_date].copy()
            if future_wk.empty:
                return None

            if side == 'long':
                cond_exit = (future_wk['prev_Close'] >= future_wk['prev_Sma5']) & (future_wk['Close'] < future_wk['Sma5'])
            else:  # short
                cond_exit = (future_wk['prev_Close'] <= future_wk['prev_Sma5']) & (future_wk['Close'] > future_wk['Sma5'])

            if not future_wk[cond_exit].empty:
                exit_row = future_wk[cond_exit].iloc[0]
            else:
                exit_row = future_wk.iloc[-1]  # 若永不觸發，最後一週強制出場

            return (exit_row['Date'], float(exit_row['Close']))

        # ── 多頭流程 ─────────────────────────────────────────────
        for idx in wk.index[wk['bull_cross']].tolist():
            entry_pack = _find_entry_in_next_week(idx, side='long')
            if entry_pack is None:
                continue
            entry_date, entry_price, trigger_date, trigger_type = entry_pack

            exit_pack = _find_exit_after(wk.loc[idx, 'Date'], side='long')
            if exit_pack is None:
                continue
            exit_date, exit_price = exit_pack

            pnl     = exit_price - entry_price
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'direction':   'long',
                'signal_week': wk.loc[idx, 'Date'],
                'trigger_date': trigger_date,
                'trigger_type': trigger_type,   # 'cross' or 'touch'
                'entry_date':  entry_date,
                'entry_price': float(entry_price),
                'exit_date':   exit_date,
                'exit_price':  float(exit_price),
                'pnl':         float(pnl),
                'pnl_pct':     float(pnl_pct),
            })

        # ── 空頭流程 ─────────────────────────────────────────────
        for idx in wk.index[wk['bear_cross']].tolist():
            entry_pack = _find_entry_in_next_week(idx, side='short')
            if entry_pack is None:
                continue
            entry_date, entry_price, trigger_date, trigger_type = entry_pack

            exit_pack = _find_exit_after(wk.loc[idx, 'Date'], side='short')
            if exit_pack is None:
                continue
            exit_date, exit_price = exit_pack

            pnl     = entry_price - exit_price     # 空頭損益
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'direction':   'short',
                'signal_week': wk.loc[idx, 'Date'],
                'trigger_date': trigger_date,
                'trigger_type': trigger_type,
                'entry_date':  entry_date,
                'entry_price': float(entry_price),
                'exit_date':   exit_date,
                'exit_price':  float(exit_price),
                'pnl':         float(pnl),
                'pnl_pct':     float(pnl_pct),
            })

        trades_df = pd.DataFrame(trades).sort_values(['entry_date', 'exit_date']).reset_index(drop=True)
        if not trades_df.empty:
            trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days
            if show_summary:
                n  = len(trades_df)
                wr = (trades_df['pnl'] > 0).mean()
                tot = (1 + trades_df['pnl_pct']/100).prod() - 1
                print(f"Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}  (long={sum(trades_df['direction']=='long')}, short={sum(trades_df['direction']=='short')})")

        return trades_df

    def backtest_sma_strategy_V4(weekly_csv, daily_csv, show_summary=False, direct_entry_no_retest=False):
        """
        進場條件（只看「訊號週」之後的『下一週』日線）：
        多頭：週 Close 上穿 週SMA5 -> 下一週日線出現
                (A) 日 Close 由下穿上 20MA，或
                (B) 日 Low ≤ 20MA 且 Close > Open（碰線後轉強）
                → 觸發日『下一個交易日開盤』進場做多
        空頭：週 Close 下穿 週SMA5 -> 下一週日線出現
                (A) 日 Close 由上穿下 20MA，或
                (B) 日 High ≥ 20MA 且 Close < Open（碰線後轉弱）
                → 觸發日『下一個交易日開盤』進場放空

        直接進場開關：
        - direct_entry_no_retest = True 時，不檢查上述(A)(B)日線條件，
            於「下一週第一個交易日『開盤』」直接進場（trigger_type='direct_no_retest'）。

        出場條件：
        多頭：第一次出現 週 Close 跌破 週SMA5 的那一週收盤。
        空頭：第一次出現 週 Close 突破 週SMA5 的那一週收盤。
        """
        import pandas as pd
        import numpy as np

        # 讀入並排序
        wk = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        dk = pd.read_csv(daily_csv,  parse_dates=['Date']).sort_values('Date').reset_index(drop=True)

        # 準備週線前一週欄位（偵測跨越）
        wk['prev_Close'] = wk['Close'].shift(1)
        wk['prev_Sma5']  = wk['Sma5'].shift(1)

        # 週線訊號
        wk['bull_cross'] = (wk['prev_Close'] < wk['prev_Sma5']) & (wk['Close'] > wk['Sma5'])  # 上穿
        wk['bear_cross'] = (wk['prev_Close'] > wk['prev_Sma5']) & (wk['Close'] < wk['Sma5'])  # 下穿

        # 日線計算 20 日
        dk['SMA20']      = dk['Close'].rolling(20, min_periods=20).mean()
        dk['prev_Close'] = dk['Close'].shift(1)
        dk['prev_SMA20'] = dk['SMA20'].shift(1)

        trades = []

        def _get_next_week_window(signal_week_idx: int):
            """回傳『下一週』在日線中的視窗 (start_exclusive, end_inclusive) 與對應日K資料。"""
            if signal_week_idx + 1 >= len(wk):
                return None, None, None
            start = wk.loc[signal_week_idx, 'Date']
            end   = wk.loc[signal_week_idx + 1, 'Date']
            dw = dk[(dk['Date'] > start) & (dk['Date'] <= end)].copy()
            return start, end, dw

        def _entry_direct_next_week(signal_week_idx: int):
            """不做日線回測：取下一週第一個交易日『開盤』進場。"""
            start, end, dw = _get_next_week_window(signal_week_idx)
            if dw is None or dw.empty:
                return None
            first_day = dw.iloc[0]
            entry_date  = first_day['Date']
            entry_price = float(first_day['Open'])
            trigger_date = entry_date
            trigger_type = 'direct_no_retest'
            return (entry_date, entry_price, trigger_date, trigger_type)

        def _find_entry_in_next_week(signal_week_idx: int, side: str):
            """
            在『下一週』日線中尋找回測20MA的觸發點並回傳 (entry_date, entry_price, trigger_date, trigger_type) 或 None
            side: 'long' or 'short'
            """
            start, end, dw = _get_next_week_window(signal_week_idx)
            if dw is None or dw.empty:
                return None

            if side == 'long':
                # (A) 上穿20MA
                cond_cross = (dw['prev_Close'] < dw['prev_SMA20']) & (dw['Close'] > dw['SMA20'])
                # (B) 碰到20MA後轉強
                cond_touch = (dw['Low'] <= dw['SMA20']) & (dw['Close'] > dw['Open'])
            else:  # short
                # (A) 下穿20MA
                cond_cross = (dw['prev_Close'] > dw['prev_SMA20']) & (dw['Close'] < dw['SMA20'])
                # (B) 碰到20MA後轉弱
                cond_touch = (dw['High'] >= dw['SMA20']) & (dw['Close'] < dw['Open'])

            dw['retest_ok'] = cond_cross | cond_touch
            ok = dw[dw['retest_ok']]
            if ok.empty:
                return None

            trigger_row = ok.iloc[0]
            trigger_date = trigger_row['Date']
            trigger_type = 'cross' if cond_cross.loc[trigger_row.name] else 'touch'

            # 進場：觸發日『下一個交易日開盤』
            future_days = dk[dk['Date'] > trigger_date]
            if future_days.empty:
                return None
            entry_row = future_days.iloc[0]
            return (entry_row['Date'], float(entry_row['Open']), trigger_date, trigger_type)

        def _find_exit_after(signal_week_date: pd.Timestamp, side: str):
            """
            依週線找第一個反向跨越的出場週（含該週的 Close 價格）
            """
            future_wk = wk[wk['Date'] > signal_week_date].copy()
            if future_wk.empty:
                return None

            if side == 'long':
                cond_exit = (future_wk['prev_Close'] >= future_wk['prev_Sma5']) & (future_wk['Close'] < future_wk['Sma5'])
            else:  # short
                cond_exit = (future_wk['prev_Close'] <= future_wk['prev_Sma5']) & (future_wk['Close'] > future_wk['Sma5'])

            if not future_wk[cond_exit].empty:
                exit_row = future_wk[cond_exit].iloc[0]
            else:
                exit_row = future_wk.iloc[-1]  # 若永不觸發，最後一週強制出場

            return (exit_row['Date'], float(exit_row['Close']))

        # ── 多頭流程 ─────────────────────────────────────────────
        for idx in wk.index[wk['bull_cross']].tolist():
            if direct_entry_no_retest:
                entry_pack = _entry_direct_next_week(idx)
            else:
                entry_pack = _find_entry_in_next_week(idx, side='long')
            if entry_pack is None:
                continue
            entry_date, entry_price, trigger_date, trigger_type = entry_pack

            exit_pack = _find_exit_after(wk.loc[idx, 'Date'], side='long')
            if exit_pack is None:
                continue
            exit_date, exit_price = exit_pack

            pnl     = exit_price - entry_price
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'direction':   'long',
                'signal_week': wk.loc[idx, 'Date'],
                'trigger_date': trigger_date,
                'trigger_type': trigger_type,   # 'cross'/'touch'/'direct_no_retest'
                'entry_date':  entry_date,
                'entry_price': float(entry_price),
                'exit_date':   exit_date,
                'exit_price':  float(exit_price),
                'pnl':         float(pnl),
                'pnl_pct':     float(pnl_pct),
            })

        # ── 空頭流程 ─────────────────────────────────────────────
        for idx in wk.index[wk['bear_cross']].tolist():
            if direct_entry_no_retest:
                entry_pack = _entry_direct_next_week(idx)
            else:
                entry_pack = _find_entry_in_next_week(idx, side='short')
            if entry_pack is None:
                continue
            entry_date, entry_price, trigger_date, trigger_type = entry_pack

            exit_pack = _find_exit_after(wk.loc[idx, 'Date'], side='short')
            if exit_pack is None:
                continue
            exit_date, exit_price = exit_pack

            pnl     = entry_price - exit_price     # 空頭損益
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'direction':   'short',
                'signal_week': wk.loc[idx, 'Date'],
                'trigger_date': trigger_date,
                'trigger_type': trigger_type,   # 'cross'/'touch'/'direct_no_retest'
                'entry_date':  entry_date,
                'entry_price': float(entry_price),
                'exit_date':   exit_date,
                'exit_price':  float(exit_price),
                'pnl':         float(pnl),
                'pnl_pct':     float(pnl_pct),
            })

        trades_df = pd.DataFrame(trades).sort_values(['entry_date', 'exit_date']).reset_index(drop=True)
        if not trades_df.empty:
            trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days
            if show_summary:
                n  = len(trades_df)
                wr = (trades_df['pnl'] > 0).mean()
                tot = (1 + trades_df['pnl_pct']/100).prod() - 1
                print(f"Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}  (long={sum(trades_df['direction']=='long')}, short={sum(trades_df['direction']=='short')})")

        return trades_df

    def backtest_sma_strategy_V5(
        weekly_csv,
        daily_csv,
        show_summary=False,
        direct_entry_no_retest=False,
        *,
        signal_tf: str = "week",     # "week" 或 "month"：用週K或月K判斷突破
        ma_days: int = 20,           # 回測/出場用的日線均線天數（原本月線=20）
        monthly_csv: str | None = None,  # signal_tf="month" 時可提供月K檔；不提供則由日線重採樣
        ):
        """
        進場邏輯（與 V5 相同但可選週/月觸發 + 自訂回測日線均線天數）：
        - 訊號： (週/月) Close 與 (週/月) Sma5 交叉（上穿=多頭、下穿=空頭）
        - 視窗：於「訊號期之後的下一期」的『日線』中找觸發
            * 回測模式：日線 (A) 與 SMA(ma_days) 交叉；或 (B) 單日觸碰後轉向（多：Low<=SMA & Close>Open；空：High>=SMA & Close<Open）
            * 直接模式：direct_entry_no_retest=True → 下一期第一個交易日『開盤』直接進場
        - 期末日線過濾（僅在“需要時”套用）：
            * 若訊號期『最後一根日K』多頭落在 SMA(ma_days) 下方（空頭在上方），則下一期觸發日必須站在正確一側（多>、空<）才能進場
        - 出場：日線對 SMA(ma_days) 的反向交叉
        回傳：trades_df
        """
        import pandas as pd
        import numpy as np

        # ========= 讀日線並計算可變天數 SMA =========
        dk = pd.read_csv(daily_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        dk[f'SMA{ma_days}']      = dk['Close'].rolling(ma_days, min_periods=ma_days).mean()
        dk['prev_Close']         = dk['Close'].shift(1)
        dk[f'prev_SMA{ma_days}'] = dk[f'SMA{ma_days}'].shift(1)

        # ========= 讀(週/月)線作為訊號時間框架 =========
        signal_tf = (signal_tf or "week").lower()
        if signal_tf not in ("week", "month"):
            raise ValueError("signal_tf 必須是 'week' 或 'month'")

        if signal_tf == "week":
            px = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        else:
            if monthly_csv:
                px = pd.read_csv(monthly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            else:
                # 由日線重採樣成月K（月末）
                d2 = dk.set_index('Date')
                px = d2.resample('M', label='right', closed='right').agg({
                    'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
                }).dropna(subset=['Open','High','Low','Close']).reset_index()

        # 若缺 Sma5 則補
        if 'Sma5' not in px.columns:
            px['Sma5'] = px['Close'].rolling(5, min_periods=5).mean()

        px['prev_Close'] = px['Close'].shift(1)
        px['prev_Sma5']  = px['Sma5'].shift(1)
        # 交叉訊號
        px['bull_cross'] = (px['prev_Close'] < px['prev_Sma5']) & (px['Close'] > px['Sma5'])
        px['bear_cross'] = (px['prev_Close'] > px['prev_Sma5']) & (px['Close'] < px['Sma5'])

        # ========= 工具：抓“某訊號期”的期末那一根日K =========
        def _period_end_daily_row(signal_idx: int):
            end_dt = px.loc[signal_idx, 'Date']
            if signal_tf == "week":
                prev_end = px.loc[signal_idx-1, 'Date'] if signal_idx-1 >= 0 else pd.Timestamp.min
                win = dk[(dk['Date'] > prev_end) & (dk['Date'] <= end_dt)]
            else:  # month
                month_key = end_dt.to_period('M')
                win = dk[dk['Date'].dt.to_period('M') == month_key]
            if win.empty:
                return None
            return win.iloc[-1]  # 期末最後一根日K

        # ========= 工具：取“下一期”的日線視窗 =========
        def _next_period_window(signal_idx: int):
            if signal_idx + 1 >= len(px):
                return None, None, None
            start = px.loc[signal_idx, 'Date']
            end   = px.loc[signal_idx+1, 'Date']
            dw = dk[(dk['Date'] > start) & (dk['Date'] <= end)].copy()
            return start, end, dw

        # ========= 進場：不回測，直接取下一期第一天開盤（套用必要的 MA 過濾） =========
        def _entry_direct_next_period(signal_idx: int, side: str, need_filter: bool):
            start, end, dw = _next_period_window(signal_idx)
            if dw is None or dw.empty:
                return None
            first_day = dw.iloc[0]
            if need_filter:
                if side == 'long' and not (first_day['Close'] > first_day[f'SMA{ma_days}']):
                    return None
                if side == 'short' and not (first_day['Close'] < first_day[f'SMA{ma_days}']):
                    return None
            entry_date  = first_day['Date']
            entry_price = float(first_day['Open'])
            trigger_date = entry_date
            trigger_type = 'direct_no_retest'
            return (entry_date, entry_price, trigger_date, trigger_type)

        # ========= 進場：下一期內找回測觸發（交叉 or 觸碰轉向） =========
        def _find_entry_in_next_period(signal_idx: int, side: str, need_filter: bool):
            start, end, dw = _next_period_window(signal_idx)
            if dw is None or dw.empty:
                return None

            # (A) 與 SMA 交叉
            if side == 'long':
                cond_cross = (dw['prev_Close'] < dw[f'prev_SMA{ma_days}']) & (dw['Close'] > dw[f'SMA{ma_days}'])
                # (B) 觸碰後轉強
                cond_touch = (dw['Low'] <= dw[f'SMA{ma_days}']) & (dw['Close'] > dw['Open'])
            else:
                cond_cross = (dw['prev_Close'] > dw[f'prev_SMA{ma_days}']) & (dw['Close'] < dw[f'SMA{ma_days}'])
                cond_touch = (dw['High'] >= dw[f'SMA{ma_days}']) & (dw['Close'] < dw['Open'])

            dw = dw.copy()
            dw['retest_ok'] = cond_cross | cond_touch

            # 必要時套用“站在正確一側”的濾網
            if need_filter:
                if side == 'long':
                    dw['retest_ok'] = dw['retest_ok'] & (dw['Close'] > dw[f'SMA{ma_days}'])
                else:
                    dw['retest_ok'] = dw['retest_ok'] & (dw['Close'] < dw[f'SMA{ma_days}'])

            ok = dw[dw['retest_ok']]
            if ok.empty:
                return None

            trig = ok.iloc[0]
            trigger_date = trig['Date']
            trigger_type = 'cross' if ((side == 'long' and cond_cross.loc[trig.name]) or
                                    (side == 'short' and cond_cross.loc[trig.name])) else 'touch'
            # 進場：觸發日「下一個交易日開盤」
            future_days = dk[dk['Date'] > trigger_date]
            if future_days.empty:
                return None
            entry_row = future_days.iloc[0]
            return (entry_row['Date'], float(entry_row['Open']), trigger_date, trigger_type)

        # ========= 出場：日線對 SMA(ma_days) 的反向交叉 =========
        def _find_exit_after_by_ma(entry_date: pd.Timestamp, side: str):
            future_d = dk[dk['Date'] > entry_date].copy()
            if future_d.empty:
                return None
            if side == 'long':
                cond = (future_d['prev_Close'] >= future_d[f'prev_SMA{ma_days}']) & (future_d['Close'] < future_d[f'SMA{ma_days}'])
            else:
                cond = (future_d['prev_Close'] <= future_d[f'prev_SMA{ma_days}']) & (future_d['Close'] > future_d[f'SMA{ma_days}'])
            if not future_d[cond].empty:
                r = future_d[cond].iloc[0]
            else:
                r = future_d.iloc[-1]  # 永不觸發→最後一天強制出場
            return (r['Date'], float(r['Close']))

        trades = []

        # ========= 多頭流程 =========
        for idx in px.index[px['bull_cross']].tolist():
            # 期末日線是否落在 SMA(ma_days) 下方？若是 → 下一期觸發日需站上 SMA
            end_row = _period_end_daily_row(idx)
            need_filter = False
            if end_row is not None and not np.isnan(end_row[f'SMA{ma_days}']):
                need_filter = bool(end_row['Close'] < end_row[f'SMA{ma_days}'])

            if direct_entry_no_retest:
                entry_pack = _entry_direct_next_period(idx, side='long', need_filter=need_filter)
            else:
                entry_pack = _find_entry_in_next_period(idx, side='long', need_filter=need_filter)
            if entry_pack is None:
                continue
            entry_date, entry_price, trigger_date, trigger_type = entry_pack

            exit_pack = _find_exit_after_by_ma(entry_date, side='long')
            if exit_pack is None:
                continue
            exit_date, exit_price = exit_pack

            pnl     = exit_price - entry_price
            pnl_pct = (pnl / entry_price) * 100.0
            trades.append({
                'direction':    'long',
                'signal_tf':    signal_tf,
                'signal_period_end': px.loc[idx, 'Date'],
                'trigger_date': trigger_date,
                'trigger_type': trigger_type,   # 'cross'/'touch'/'direct_no_retest'
                'entry_date':   entry_date,
                'entry_price':  float(entry_price),
                'exit_date':    exit_date,
                'exit_price':   float(exit_price),
                'pnl':          float(pnl),
                'pnl_pct':      float(pnl_pct),
                'ma_days':      ma_days,
            })

        # ========= 空頭流程 =========
        for idx in px.index[px['bear_cross']].tolist():
            # 期末日線是否落在 SMA(ma_days) 上方？若是 → 下一期觸發日需站回 SMA 下方
            end_row = _period_end_daily_row(idx)
            need_filter = False
            if end_row is not None and not np.isnan(end_row[f'SMA{ma_days}']):
                need_filter = bool(end_row['Close'] > end_row[f'SMA{ma_days}'])

            if direct_entry_no_retest:
                entry_pack = _entry_direct_next_period(idx, side='short', need_filter=need_filter)
            else:
                entry_pack = _find_entry_in_next_period(idx, side='short', need_filter=need_filter)
            if entry_pack is None:
                continue
            entry_date, entry_price, trigger_date, trigger_type = entry_pack

            exit_pack = _find_exit_after_by_ma(entry_date, side='short')
            if exit_pack is None:
                continue
            exit_date, exit_price = exit_pack

            pnl     = entry_price - exit_price
            pnl_pct = (pnl / entry_price) * 100.0
            trades.append({
                'direction':    'short',
                'signal_tf':    signal_tf,
                'signal_period_end': px.loc[idx, 'Date'],
                'trigger_date': trigger_date,
                'trigger_type': trigger_type,
                'entry_date':   entry_date,
                'entry_price':  float(entry_price),
                'exit_date':    exit_date,
                'exit_price':   float(exit_price),
                'pnl':          float(pnl),
                'pnl_pct':      float(pnl_pct),
                'ma_days':      ma_days,
            })

        # ========= 收尾 =========
        import pandas as pd
        trades_df = pd.DataFrame(trades)
        if trades_df.empty:
            if show_summary:
                print("No trades generated.")
            return trades_df

        trades_df = trades_df.sort_values(['entry_date','exit_date']).reset_index(drop=True)
        trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days

        if show_summary:
            n  = len(trades_df)
            wr = (trades_df['pnl'] > 0).mean()
            tot = (1 + trades_df['pnl_pct']/100).prod() - 1
            print(f"Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}  "
                f"(long={sum(trades_df['direction']=='long')}, short={sum(trades_df['direction']=='short')})")

        return trades_df

    def backtest_sma_strategy_V6(
        weekly_csv,
        daily_csv,
        show_summary=False,
        direct_entry_no_retest=False,
        *,
        signal_tf="week",      # "week" 或 "month"：用週K或月K判斷突破
        ma_days=20,            # 回測/出場用的「日線」均線天數
        retest_tf="day",     # "day" 或 "weekly"：回測觸發時間框架
        retest_ma=None,        # 回測用「週線」均線長度（retest_tf="weekly" 時生效；預設 4）
        monthly_csv=None,      # signal_tf="month" 時可提供月K檔；不提供則由日線重採樣
        tp_pct=None,           # ★ 停利百分比（例如 0.03=3%）。None 表示不啟用
        ):
        """
        出場：取最先發生者
        1) 達成停利（tp_pct）；
        2) 日線對 SMA(ma_days) 的反向交叉。
        其餘規則同前一版。
        """
        import pandas as pd
        import numpy as np

        # ===== 日線：讀入 + 日SMA =====
        dk = pd.read_csv(daily_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        dk[f'SMA{ma_days}']      = dk['Close'].rolling(ma_days, min_periods=ma_days).mean()
        dk['prev_Close']         = dk['Close'].shift(1)
        dk[f'prev_SMA{ma_days}'] = dk[f'SMA{ma_days}'].shift(1)

        # ===== 訊號時間框架（週/月）資料 =====
        signal_tf = (signal_tf or "week").lower()
        if signal_tf not in ("week", "month"):
            raise ValueError("signal_tf 必須是 'week' 或 'month'")

        if signal_tf == "week":
            px = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        else:
            if monthly_csv:
                px = pd.read_csv(monthly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            else:
                d2 = dk.set_index('Date')
                px = d2.resample('M', label='right', closed='right').agg({
                    'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
                }).dropna(subset=['Open','High','Low','Close']).reset_index()

        if 'Sma5' not in px.columns:
            px['Sma5'] = px['Close'].rolling(5, min_periods=5).mean()
        px['prev_Close'] = px['Close'].shift(1)
        px['prev_Sma5']  = px['Sma5'].shift(1)
        px['bull_cross'] = (px['prev_Close'] < px['prev_Sma5']) & (px['Close'] > px['Sma5'])
        px['bear_cross'] = (px['prev_Close'] > px['prev_Sma5']) & (px['Close'] < px['Sma5'])

        # ===== 周線回測所需的周資料（由日線重採樣） =====
        retest_tf = (retest_tf or "day").lower()
        if retest_tf not in ("day", "week"):
            raise ValueError("retest_tf 必須是 'day' 或 'week'")

        wk_from_d = None
        if retest_tf == "week":
            retest_ma = int(retest_ma or 4)  # 預設 4 週 ≈ 20 交易日
            d3 = dk.set_index('Date')
            wk_from_d = d3.resample('W-FRI', label='right', closed='right').agg({
                'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
            }).dropna(subset=['Open','High','Low','Close']).reset_index()
            wk_from_d['W_SMA']      = wk_from_d['Close'].rolling(retest_ma, min_periods=retest_ma).mean()
            wk_from_d['prev_Close'] = wk_from_d['Close'].shift(1)
            wk_from_d['prev_W_SMA'] = wk_from_d['W_SMA'].shift(1)

        # ===== helper：取下一期（日視窗 & 週視窗） =====
        def _next_period_bounds(i):
            if i + 1 >= len(px): return None, None
            return px.loc[i, 'Date'], px.loc[i+1, 'Date']

        def _next_period_days(i):
            b = _next_period_bounds(i)
            if b == (None, None): return None
            start, end = b
            return dk[(dk['Date'] > start) & (dk['Date'] <= end)].copy()

        def _next_period_weeks(i):
            if wk_from_d is None: return None
            b = _next_period_bounds(i)
            if b == (None, None): return None
            start, end = b
            return wk_from_d[(wk_from_d['Date'] > start) & (wk_from_d['Date'] <= end)].copy()

        # ===== 期末濾網（僅在 retest_tf='day' 時套用）=====
        def _need_daily_filter(i, side: str) -> bool:
            end_dt = px.loc[i, 'Date']
            if signal_tf == "week":
                prev_end = px.loc[i-1, 'Date'] if i-1 >= 0 else pd.Timestamp.min
                win = dk[(dk['Date'] > prev_end) & (dk['Date'] <= end_dt)]
            else:
                mkey = end_dt.to_period('M')
                win = dk[dk['Date'].dt.to_period('M') == mkey]
            if win.empty: return False
            last_day = win.iloc[-1]
            if np.isnan(last_day[f'SMA{ma_days}']): return False
            if side == 'long':
                return bool(last_day['Close'] < last_day[f'SMA{ma_days}'])
            else:
                return bool(last_day['Close'] > last_day[f'SMA{ma_days}'])

        # ===== 直接進場（下一期第一天開盤）=====
        def _entry_direct(i, side: str):
            dw = _next_period_days(i)
            if dw is None or dw.empty: return None
            first_day = dw.iloc[0]
            if retest_tf == 'day':
                need = _need_daily_filter(i, side)
                if need:
                    if side == 'long' and not (first_day['Close'] > first_day[f'SMA{ma_days}']): return None
                    if side == 'short' and not (first_day['Close'] < first_day[f'SMA{ma_days}']): return None
            return (first_day['Date'], float(first_day['Open']), first_day['Date'], 'direct_no_retest')

        # ===== 回測觸發（day）=====
        def _entry_retest_daily(i, side: str):
            dw = _next_period_days(i)
            if dw is None or dw.empty: return None
            if side == 'long':
                cond_cross = (dw['prev_Close'] < dw[f'prev_SMA{ma_days}']) & (dw['Close'] > dw[f'SMA{ma_days}'])
                cond_touch = (dw['Low'] <= dw[f'SMA{ma_days}']) & (dw['Close'] > dw['Open'])
            else:
                cond_cross = (dw['prev_Close'] > dw[f'prev_SMA{ma_days}']) & (dw['Close'] < dw[f'SMA{ma_days}'])
                cond_touch = (dw['High'] >= dw[f'SMA{ma_days}']) & (dw['Close'] < dw['Open'])
            dw = dw.copy()
            dw['retest_ok'] = cond_cross | cond_touch
            need = _need_daily_filter(i, side)
            if need:
                if side == 'long':
                    dw['retest_ok'] = dw['retest_ok'] & (dw['Close'] > dw[f'SMA{ma_days}'])
                else:
                    dw['retest_ok'] = dw['retest_ok'] & (dw['Close'] < dw[f'SMA{ma_days}'])
            ok = dw[dw['retest_ok']]
            if ok.empty: return None
            trig = ok.iloc[0]
            trigger_date = trig['Date']
            trigger_type = 'cross' if ((side == 'long' and cond_cross.loc[trig.name]) or
                                    (side == 'short' and cond_cross.loc[trig.name])) else 'touch'
            future_days = dk[dk['Date'] > trigger_date]
            if future_days.empty: return None
            entry_row = future_days.iloc[0]
            return (entry_row['Date'], float(entry_row['Open']), trigger_date, trigger_type)

        # ===== 回測觸發（weekly）=====
        def _entry_retest_weekly(i, side: str):
            ww = _next_period_weeks(i)
            if ww is None or ww.empty: return None
            if side == 'long':
                cond_cross = (ww['prev_Close'] < ww['prev_W_SMA']) & (ww['Close'] > ww['W_SMA'])
                cond_touch = (ww['Low'] <= ww['W_SMA']) & (ww['Close'] > ww['Open'])
            else:
                cond_cross = (ww['prev_Close'] > ww['prev_W_SMA']) & (ww['Close'] < ww['W_SMA'])
                cond_touch = (ww['High'] >= ww['W_SMA']) & (ww['Close'] < ww['Open'])
            ww = ww.copy()
            ww['retest_ok'] = cond_cross | cond_touch
            ok = ww[ww['retest_ok']]
            if ok.empty: return None
            trig = ok.iloc[0]
            trigger_week_end = trig['Date']
            future_days = dk[dk['Date'] > trigger_week_end]
            if future_days.empty: return None
            entry_row = future_days.iloc[0]
            trigger_type = 'W_cross' if ((side == 'long' and cond_cross.loc[trig.name]) or
                                        (side == 'short' and cond_cross.loc[trig.name])) else 'W_touch'
            return (entry_row['Date'], float(entry_row['Open']), trigger_week_end, trigger_type)

        # ===== 出場：先看停利，再看 MA 反向交叉 =====
        def _exit_with_tp(entry_date: pd.Timestamp, side: str, entry_price: float):
            future = dk[dk['Date'] > entry_date].copy()
            if future.empty: return None
            target = None
            if tp_pct is not None and tp_pct > 0:
                target = entry_price * (1.0 + tp_pct) if side == 'long' else entry_price * (1.0 - tp_pct)

            for _, r in future.iterrows():
                c = float(r['Close'])
                # 1) 停利先判斷（達成即出場）
                if target is not None:
                    if (side == 'long' and c >= target) or (side == 'short' and c <= target):
                        return (r['Date'], c, 'TP_pct')
                # 2) 日線對 SMA(ma_days) 的反向交叉
                prev_c = float(r['prev_Close'])
                prev_s = float(r[f'prev_SMA{ma_days}'])
                s_now  = float(r[f'SMA{ma_days}'])
                if side == 'long':
                    if (prev_c >= prev_s) and (c < s_now):
                        return (r['Date'], c, 'MA_cross')
                else:
                    if (prev_c <= prev_s) and (c > s_now):
                        return (r['Date'], c, 'MA_cross')

            # 都沒觸發 → 最後一天強制出場
            r = future.iloc[-1]
            return (r['Date'], float(r['Close']), 'FORCED_LAST')

        trades = []

        # ===== 掃描多空 =====
        for side, mask_col in (('long','bull_cross'), ('short','bear_cross')):
            for idx in px.index[px[mask_col]].tolist():
                # 進場
                if direct_entry_no_retest:
                    entry_pack = _entry_direct(idx, side)
                else:
                    entry_pack = _entry_retest_daily(idx, side) if retest_tf=='day' else _entry_retest_weekly(idx, side)
                if entry_pack is None:
                    continue

                entry_date, entry_price, trigger_date, trigger_type = entry_pack
                exit_pack = _exit_with_tp(entry_date, side, entry_price)
                if exit_pack is None:
                    continue
                exit_date, exit_price, exit_reason = exit_pack

                pnl     = (exit_price - entry_price) if side=='long' else (entry_price - exit_price)
                pnl_pct = (pnl / entry_price) * 100.0
                trades.append({
                    'direction': side,
                    'signal_tf': signal_tf,
                    'retest_tf': retest_tf,
                    'ma_days':   ma_days,
                    'retest_ma': (None if retest_tf=='day' else int(retest_ma or 4)),
                    'tp_pct':    (None if tp_pct is None else float(tp_pct)),
                    'signal_period_end': px.loc[idx, 'Date'],
                    'trigger_date': trigger_date,
                    'trigger_type': trigger_type,   # cross/touch 或 W_cross/W_touch/direct_no_retest
                    'entry_date':   entry_date,
                    'entry_price':  float(entry_price),
                    'exit_date':    exit_date,
                    'exit_price':   float(exit_price),
                    'exit_reason':  exit_reason,    # 'TP_pct' / 'MA_cross' / 'FORCED_LAST'
                    'pnl':          float(pnl),
                    'pnl_pct':      float(pnl_pct),
                })

        # ===== 收尾 =====
        trades_df = pd.DataFrame(trades)
        if trades_df.empty:
            if show_summary:
                print("No trades generated.")
            return trades_df

        trades_df = trades_df.sort_values(['entry_date','exit_date']).reset_index(drop=True)
        trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days

        if show_summary:
            n  = len(trades_df)
            wr = (trades_df['pnl'] > 0).mean()
            tot = (1 + trades_df['pnl_pct']/100).prod() - 1
            print(f"Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}  "
                f"(long={sum(trades_df['direction']=='long')}, short={sum(trades_df['direction']=='short')})")

        return trades_df

    def backtest_sma_strategy_V7(weekly_csv, daily_csv, show_summary=False, direct_entry_no_retest=False):
        """
        進場條件（新的規則）：
        只要在5周K上就做多
        多頭：當週週K收在週SMA5之上 -> 「下一週」日線若出現回測20MA（Low ≤ SMA20）→ 回測日『下一個交易日開盤』進場做多
        空頭：當週週K收在週SMA5之下 -> 「下一週」日線若出現回測20MA（High ≥ SMA20）→ 回測日『下一個交易日開盤』進場放空

        直接進場開關（相容舊參數）：
        - direct_entry_no_retest = True 時，不檢查日線回測條件，
        於「下一週第一個交易日『開盤』」直接進場（trigger_type='direct_no_retest'）。

        出場條件（維持舊規則）：
        多頭：第一次出現 週 Close 跌破 週SMA5 的那一週收盤。
        空頭：第一次出現 週 Close 突破 週SMA5 的那一週收盤。
        """
        import pandas as pd
        import numpy as np

        # 讀入並排序
        wk = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        dk = pd.read_csv(daily_csv,  parse_dates=['Date']).sort_values('Date').reset_index(drop=True)

        # 準備週線前一週欄位（供出場偵測用）
        wk['prev_Close'] = wk['Close'].shift(1)
        wk['prev_Sma5']  = wk['Sma5'].shift(1)

        # 日線計算 20 日
        dk['SMA20']      = dk['Close'].rolling(20, min_periods=20).mean()

        trades = []

        def _get_next_week_window(signal_week_idx: int):
            """回傳『下一週』在日線中的視窗 (start_exclusive, end_inclusive) 與對應日K資料。"""
            if signal_week_idx + 1 >= len(wk):
                return None, None, None
            start = wk.loc[signal_week_idx, 'Date']
            end   = wk.loc[signal_week_idx + 1, 'Date']
            dw = dk[(dk['Date'] > start) & (dk['Date'] <= end)].copy()
            return start, end, dw

        def _entry_direct_next_week(signal_week_idx: int):
            """不做日線回測：取下一週第一個交易日『開盤』進場。"""
            start, end, dw = _get_next_week_window(signal_week_idx)
            if dw is None or dw.empty:
                return None
            first_day = dw.iloc[0]
            entry_date  = first_day['Date']
            entry_price = float(first_day['Open'])
            trigger_date = entry_date
            trigger_type = 'direct_no_retest'
            return (entry_date, entry_price, trigger_date, trigger_type)

        def _find_entry_in_next_week_by_retest(signal_week_idx: int, side: str):
            """
            在『下一週』日線中尋找回測20MA的觸發點並回傳
            (entry_date, entry_price, trigger_date, trigger_type) 或 None
            回測定義：
            long  : Low ≤ SMA20
            short : High ≥ SMA20
            進場：觸發日『下一個交易日開盤』
            """
            start, end, dw = _get_next_week_window(signal_week_idx)
            if dw is None or dw.empty:
                return None

            # 只在SMA20已形成的情況下判定回測
            dw = dw.copy()
            dw = dw[~dw['SMA20'].isna()]
            if dw.empty:
                return None

            if side == 'long':
                cond_retest = dw['Low'] <= dw['SMA20']
            else:
                cond_retest = dw['High'] >= dw['SMA20']

            ok = dw[cond_retest]
            if ok.empty:
                return None

            trigger_row  = ok.iloc[0]
            trigger_date = trigger_row['Date']
            trigger_type = 'retest20'

            future_days = dk[dk['Date'] > trigger_date]
            if future_days.empty:
                return None
            entry_row = future_days.iloc[0]
            return (entry_row['Date'], float(entry_row['Open']), trigger_date, trigger_type)

        def _find_exit_after(signal_week_date: pd.Timestamp, side: str):
            """
            依週線找第一個反向跨越的出場週（含該週的 Close 價格）
            """
            future_wk = wk[wk['Date'] > signal_week_date].copy()
            if future_wk.empty:
                return None

            if side == 'long':
                # 首次「跌破」SMA5 的那週收盤
                cond_exit = (future_wk['prev_Close'] >= future_wk['prev_Sma5']) & (future_wk['Close'] < future_wk['Sma5'])
            else:
                # 首次「突破」SMA5 的那週收盤
                cond_exit = (future_wk['prev_Close'] <= future_wk['prev_Sma5']) & (future_wk['Close'] > future_wk['Sma5'])

            if not future_wk[cond_exit].empty:
                exit_row = future_wk[cond_exit].iloc[0]
            else:
                exit_row = future_wk.iloc[-1]  # 若永不觸發，最後一週強制出場

            return (exit_row['Date'], float(exit_row['Close']))

        # ── 生成「狀態型」訊號週：收盤在SMA5之上/之下 ─────────────────────────
        wk['above_sma5'] = wk['Close'] > wk['Sma5']
        wk['below_sma5'] = wk['Close'] < wk['Sma5']

        # ── 多頭流程（當週在SMA5之上，下一週找日線回測20MA）─────────────────────
        for idx in wk.index[wk['above_sma5']].tolist():
            if direct_entry_no_retest:
                entry_pack = _entry_direct_next_week(idx)
            else:
                entry_pack = _find_entry_in_next_week_by_retest(idx, side='long')
            if entry_pack is None:
                continue
            entry_date, entry_price, trigger_date, trigger_type = entry_pack

            exit_pack = _find_exit_after(wk.loc[idx, 'Date'], side='long')
            if exit_pack is None:
                continue
            exit_date, exit_price = exit_pack

            pnl     = exit_price - entry_price
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'direction':   'long',
                'signal_week': wk.loc[idx, 'Date'],
                'trigger_date': trigger_date,
                'trigger_type': trigger_type,   # 'retest20' / 'direct_no_retest'
                'entry_date':  entry_date,
                'entry_price': float(entry_price),
                'exit_date':   exit_date,
                'exit_price':  float(exit_price),
                'pnl':         float(pnl),
                'pnl_pct':     float(pnl_pct),
            })

        # ── 空頭流程（當週在SMA5之下，下一週找日線回測20MA）─────────────────────
        for idx in wk.index[wk['below_sma5']].tolist():
            if direct_entry_no_retest:
                entry_pack = _entry_direct_next_week(idx)
            else:
                entry_pack = _find_entry_in_next_week_by_retest(idx, side='short')
            if entry_pack is None:
                continue
            entry_date, entry_price, trigger_date, trigger_type = entry_pack

            exit_pack = _find_exit_after(wk.loc[idx, 'Date'], side='short')
            if exit_pack is None:
                continue
            exit_date, exit_price = exit_pack

            pnl     = entry_price - exit_price     # 空頭損益
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'direction':   'short',
                'signal_week': wk.loc[idx, 'Date'],
                'trigger_date': trigger_date,
                'trigger_type': trigger_type,   # 'retest20' / 'direct_no_retest'
                'entry_date':  entry_date,
                'entry_price': float(entry_price),
                'exit_date':   exit_date,
                'exit_price':  float(exit_price),
                'pnl':         float(pnl),
                'pnl_pct':     float(pnl_pct),
            })

        trades_df = pd.DataFrame(trades).sort_values(['entry_date', 'exit_date']).reset_index(drop=True)
        if not trades_df.empty:
            trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days
            if show_summary:
                n   = len(trades_df)
                wr  = (trades_df['pnl'] > 0).mean()
                tot = (1 + trades_df['pnl_pct']/100).prod() - 1
                print(f"Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}  "
                    f"(long={sum(trades_df['direction']=='long')}, short={sum(trades_df['direction']=='short')})")

        return trades_df

    def backtest_sma_strategy_V8(
            weekly_csv, daily_csv,
            show_summary=False,
            # 進場相關
            direct_entry_no_retest=False,   # True：下一週第一天開盤直接進場
            touch_tol=0.0,                  # 觸碰均線容忍度（多：Low ≤ DMA*(1+tol)；空：High ≥ DMA*(1−tol)）
            prefer_mode='A_then_B',         # 'A_then_B' 或 'B_then_A'
            entry_at='next_open',           # 'next_open' 或 'close'
            # 週/日均線欄位（來自 CSV）
            weekly_ma_col='Sma5',
            daily_ma_col='Sma20',
            # 訊號週判定
            use_state_change_only=False     # True：只在狀態變化週觸發
        ):
            """
            依賴 CSV 內已算好的均線欄位：
            - 週線均線欄位 weekly_ma_col（預設 'Sma5'）
            - 日線均線欄位 daily_ma_col（預設 'SMA20'）

            流程：
            1) 訊號週（週K）：
            - long: 當週 Close > 週均線；short: 當週 Close < 週均線
            - use_state_change_only=True 時，只在由下→上 / 由上→下 當週觸發
            2) 進場（下一週日K）：
            - 轉折判定（相對 daily_ma_col）：
                A_touch_bounce：
                long : Low ≤ DMA*(1+tol) 且 Close>Open 且 Close>DMA
                short: High ≥ DMA*(1−tol) 且 Close<Open 且 Close<DMA
                B_two_day：
                long : Day1(收黑且在 DMA 下) → Day2(收紅且在 DMA 上)
                short: Day1(收紅且在 DMA 上) → Day2(收黑且在 DMA 下)
            - 命中後 entry_at='next_open' 用觸發日下一交易日開盤；'close' 用觸發日收盤
            - direct_entry_no_retest=True 時，忽略日線檢查，直接於下一週第一天開盤進場
            3) 出場（從 entry_date 之後掃日K）：
            - long : prev_Close ≥ prev_DMA 且 Close < DMA（向下穿越）
            - short: prev_Close ≤ prev_DMA 且 Close > DMA（向上穿越）
            - 未觸發則最後一個交易日收盤強制出場
            """
            import pandas as pd

            # ---------- 讀檔 ----------
            wk = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            dk = pd.read_csv(daily_csv,  parse_dates=['Date']).sort_values('Date').reset_index(drop=True)

            # ---------- 欄位檢查 ----------
            for col in ['Date', 'Close', weekly_ma_col]:
                if col not in wk.columns:
                    raise ValueError(f"Weekly CSV 缺少欄位：{col}")
            for col in ['Date', 'Open', 'High', 'Low', 'Close', daily_ma_col]:
                if col not in dk.columns:
                    raise ValueError(f"Daily CSV 缺少欄位：{col}")

            # ---------- 前值與輔助欄位 ----------
            wk['prev_Close'] = wk['Close'].shift(1)
            wk['prev_WMA']   = wk[weekly_ma_col].shift(1)

            dk['prev_Close'] = dk['Close'].shift(1)
            dk['prev_DMA']   = dk[daily_ma_col].shift(1)

            trades = []

            # ---------- 下一週視窗 ----------
            def _get_next_week_window(signal_week_idx: int):
                if signal_week_idx + 1 >= len(wk):
                    return None, None, None
                start = wk.loc[signal_week_idx, 'Date']
                end   = wk.loc[signal_week_idx + 1, 'Date']
                dw = dk[(dk['Date'] > start) & (dk['Date'] <= end)].copy()
                return start, end, dw

            # ---------- 直接進場 ----------
            def _entry_direct_next_week(signal_week_idx: int):
                start, end, dw = _get_next_week_window(signal_week_idx)
                if dw is None or dw.empty:
                    return None
                first_day = dw.iloc[0]
                return (first_day['Date'], float(first_day['Open']), first_day['Date'], 'direct_no_retest')

            # ---------- 轉折進場（A_touch_bounce / B_two_day） ----------
            def _find_entry_in_next_week_by_turn(signal_week_idx: int, side: str):
                start, end, dw = _get_next_week_window(signal_week_idx)
                if dw is None or dw.empty:
                    return None
                # 需要 daily_ma_col 可用
                w = dw.dropna(subset=['Open','High','Low','Close', daily_ma_col]).copy()
                if w.empty:
                    return None

                DMA = w[daily_ma_col]

                # A：同日觸碰 + 轉折
                if side == 'long':
                    condA = (w['Low']  <= DMA * (1.0 + touch_tol)) & (w['Close'] > w['Open']) & (w['Close'] > DMA)
                else:
                    condA = (w['High'] >= DMA * (1.0 - touch_tol)) & (w['Close'] < w['Open']) & (w['Close'] < DMA)

                # B：兩日翻轉（向量化）
                d1 = w.shift(1)
                DMA1 = d1[daily_ma_col]
                if side == 'long':
                    condB = (d1['Open'] > d1['Close']) & (d1['Close'] < DMA1) & (w['Open'] < w['Close']) & (w['Close'] > DMA)
                else:
                    condB = (d1['Open'] < d1['Close']) & (d1['Close'] > DMA1) & (w['Open'] > w['Close']) & (w['Close'] < DMA)

                def _pick(mask, mode_name: str):
                    hit = w[mask]
                    if hit.empty:
                        return None
                    r = hit.iloc[0]
                    trigger_date = r['Date']
                    if entry_at == 'next_open':
                        fut = dk[(dk['Date'] > trigger_date)]
                        if fut.empty:
                            return None
                        e = fut.iloc[0]
                        return (e['Date'], float(e['Open']), trigger_date, mode_name)
                    else:
                        return (trigger_date, float(r['Close']), trigger_date, mode_name)

                if prefer_mode == 'A_then_B':
                    ans = _pick(condA, 'A_touch_bounce') or _pick(condB, 'B_two_day')
                else:
                    ans = _pick(condB, 'B_two_day') or _pick(condA, 'A_touch_bounce')
                return ans

            # ---------- 出場：日線對 DMA 反向穿越 ----------
            def _find_exit_after_entry_daily(entry_date, side: str):
                future = dk[dk['Date'] > entry_date].copy()
                future = future.dropna(subset=['Close', daily_ma_col, 'prev_Close', 'prev_DMA'])
                if future.empty:
                    if dk.empty:
                        return None
                    lr = dk.dropna(subset=['Close']).iloc[-1]
                    return (lr['Date'], float(lr['Close']))

                if side == 'long':
                    cond_exit = (future['prev_Close'] >= future['prev_DMA']) & (future['Close'] < future[daily_ma_col])
                else:
                    cond_exit = (future['prev_Close'] <= future['prev_DMA']) & (future['Close'] > future[daily_ma_col])

                hit = future[cond_exit]
                er = hit.iloc[0] if not hit.empty else future.iloc[-1]
                return (er['Date'], float(er['Close']))

            # ---------- 訊號週（週線） ----------
            if use_state_change_only:
                wk['long_signal_week']  = (wk['Close'] > wk[weekly_ma_col]) & ((wk['prev_Close'] <= wk['prev_WMA']) | wk['prev_WMA'].isna())
                wk['short_signal_week'] = (wk['Close'] < wk[weekly_ma_col]) & ((wk['prev_Close'] >= wk['prev_WMA']) | wk['prev_WMA'].isna())
                long_idxs  = wk.index[wk['long_signal_week']]
                short_idxs = wk.index[wk['short_signal_week']]
            else:
                wk['above_wma'] = wk['Close'] > wk[weekly_ma_col]
                wk['below_wma'] = wk['Close'] < wk[weekly_ma_col]
                long_idxs  = wk.index[wk['above_wma']]
                short_idxs = wk.index[wk['below_wma']]

            # ---------- 建倉：多頭 ----------
            for idx in long_idxs.tolist():
                if direct_entry_no_retest:
                    entry_pack = _entry_direct_next_week(idx)
                else:
                    entry_pack = _find_entry_in_next_week_by_turn(idx, side='long')
                if entry_pack is None:
                    continue
                entry_date, entry_price, trigger_date, trigger_type = entry_pack

                exit_pack = _find_exit_after_entry_daily(entry_date, side='long')
                if exit_pack is None:
                    continue
                exit_date, exit_price = exit_pack

                pnl     = exit_price - entry_price
                pnl_pct = (pnl / entry_price) * 100.0

                trades.append({
                    'direction':   'long',
                    'signal_week': wk.loc[idx, 'Date'],
                    'trigger_date': trigger_date,
                    'trigger_type': trigger_type,
                    'entry_date':  entry_date,
                    'entry_price': float(entry_price),
                    'exit_date':   exit_date,
                    'exit_price':  float(exit_price),
                    'pnl':         float(pnl),
                    'pnl_pct':     float(pnl_pct),
                })

            # ---------- 建倉：空頭 ----------
            for idx in short_idxs.tolist():
                if direct_entry_no_retest:
                    entry_pack = _entry_direct_next_week(idx)
                else:
                    entry_pack = _find_entry_in_next_week_by_turn(idx, side='short')
                if entry_pack is None:
                    continue
                entry_date, entry_price, trigger_date, trigger_type = entry_pack

                exit_pack = _find_exit_after_entry_daily(entry_date, side='short')
                if exit_pack is None:
                    continue
                exit_date, exit_price = exit_pack

                pnl     = entry_price - exit_price   # 空頭損益
                pnl_pct = (pnl / entry_price) * 100.0

                trades.append({
                    'direction':   'short',
                    'signal_week': wk.loc[idx, 'Date'],
                    'trigger_date': trigger_date,
                    'trigger_type': trigger_type,
                    'entry_date':  entry_date,
                    'entry_price': float(entry_price),
                    'exit_date':   exit_date,
                    'exit_price':  float(exit_price),
                    'pnl':         float(pnl),
                    'pnl_pct':     float(pnl_pct),
                })

            # ---------- 輸出 ----------
            trades_df = pd.DataFrame(trades).sort_values(['entry_date', 'exit_date']).reset_index(drop=True)
            if not trades_df.empty:
                trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days
                if show_summary:
                    n   = len(trades_df)
                    wr  = (trades_df['pnl'] > 0).mean()
                    tot = (1 + trades_df['pnl_pct']/100).prod() - 1
                    print(f"Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}  "
                        f"(long={sum(trades_df['direction']=='long')}, short={sum(trades_df['direction']=='short')})")

            return trades_df

    def backtest_candle_turn_strategy_v2(
        weekly_csv,
        daily_csv,
        *,
        symbol=None,
        tp_pct: float = 0.03,     # 停利 3%
        retest_tol: float = 0.0,  # 回測容忍(比例)，例 0.001 = 0.1%
        show_summary: bool = False,
        ):
        """
        轉折價 L 的新定義：
        - high 轉折(壓力)：L = min(前週收盤, 本週收盤)
        - low  轉折(支撐)：L = max(前週收盤, 本週收盤)

        其他規則：僅在突破/跌破週的下一週以日線回測進場（單日觸碰或兩日形態）；
        進場日=觸發日、進場價=觸發價(收盤)；停利 tp_pct；停損固定用該筆交易的 L；
        另回傳 levels_df（每個轉折的失效週）。
        """
        import pandas as pd
        import numpy as np
        from pathlib import Path

        # ---------- 讀檔 & 排序 ----------
        wk = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        dk = pd.read_csv(daily_csv,  parse_dates=['Date']).sort_values('Date').reset_index(drop=True)

        # ---------- 週轉折（顏色翻轉）+ 新 L 定義 ----------
        def _find_candle_turns(wk_df: pd.DataFrame) -> pd.DataFrame:
            turns = []
            for i in range(1, len(wk_df)):
                prev = wk_df.loc[i-1]
                curr = wk_df.loc[i]
                prev_up   = prev['Close'] > prev['Open']  # 前週收漲
                prev_down = prev['Close'] < prev['Open']  # 前週收跌
                curr_up   = curr['Close'] > curr['Open']  # 本週收漲
                curr_down = curr['Close'] < curr['Open']  # 本週收跌

                if prev_up and curr_down:
                    # high 轉折 → 壓力；L = 較低（min）之收盤
                    L = float(max(prev['Close'], curr['Open']))
                    turns.append({'date': curr['Date'], 'type': 'high', 'price': L})
                if prev_down and curr_up:
                    # low 轉折 → 支撐；L = 較高（max）之收盤
                    L = float(min(prev['Close'], curr['Open']))
                    turns.append({'date': curr['Date'], 'type': 'low',  'price': L})

            return pd.DataFrame(turns, columns=['date','type','price']) if len(turns) else pd.DataFrame(columns=['date','type','price'])

        turns = _find_candle_turns(wk)

        # ---------- 轉折失效表（levels_df） ----------
        def _build_levels_table(wk_df: pd.DataFrame, turns_df: pd.DataFrame) -> pd.DataFrame:
            rows = []
            idx_map = {wk_df.loc[i, 'Date']: i for i in range(len(wk_df))}
            for _, t in turns_df.iterrows():
                turn_week_date = pd.to_datetime(t['date'])
                level_type = 'resistance' if t['type'] == 'high' else 'support'
                L = float(t['price'])
                i0 = idx_map.get(turn_week_date, None)
                invalid_week = pd.NaT
                invalid_reason = ""
                if i0 is not None:
                    for j in range(i0+1, len(wk_df)):
                        c = float(wk_df.loc[j, 'Close'])
                        d = wk_df.loc[j, 'Date']
                        if level_type == 'support':
                            if c < L:                      # 支撐失效
                                invalid_week  = d
                                invalid_reason= 'close_below_support'
                                break
                        else:
                            if c > L:                      # 壓力失效
                                invalid_week  = d
                                invalid_reason= 'close_above_resistance'
                                break
                rows.append({
                    'turn_week_date': turn_week_date,
                    'level_type': level_type,     # 'support' / 'resistance'
                    'level_price': L,
                    'invalid_week': invalid_week, # NaT 代表未失效
                    'invalid_reason': invalid_reason,
                    'still_valid': pd.isna(invalid_week)
                })
            return pd.DataFrame(rows).sort_values('turn_week_date').reset_index(drop=True)

        levels_df = _build_levels_table(wk, turns)

        # ---------- 推斷 symbol（可選） ----------
        def _infer_symbol(sym, wk_path, dk_path):
            if sym: return str(sym)
            for p in [wk_path, dk_path]:
                name = Path(p).stem
                if name: return name
            return ""
        sym = _infer_symbol(symbol, weekly_csv, daily_csv)

        # ---------- 主交易流程 ----------
        trades = []
        tol_up = 1.0 + float(retest_tol)
        tol_dn = 1.0 - float(retest_tol)

        for _, t in turns.iterrows():
            turn_week_date = pd.to_datetime(t['date'])
            turn_type = t['type']            # 'high'(壓力) / 'low'(支撐)
            L = float(t['price'])            # ★ 以新定義計算的轉折價

            # 轉折週索引
            wk_row = wk[wk['Date'] == turn_week_date]
            if wk_row.empty:
                continue
            idx = wk_row.index[0]
            if idx + 1 >= len(wk):
                continue

            # 1) 找到「首次」突破/跌破的週（breakout_week）
            future_wk = wk.iloc[idx+1:].copy()
            if turn_type == 'high':
                hit_wk = future_wk[future_wk['Close'] > L]  # 週收盤 > L ⇒ 突破 → 多頭
                side = 'long'
            else:
                hit_wk = future_wk[future_wk['Close'] < L]  # 週收盤 < L ⇒ 跌破 → 空頭
                side = 'short'
            if hit_wk.empty:
                continue
            breakout_week = pd.to_datetime(hit_wk.iloc[0]['Date'])

            # 2) 僅在「breakout_week 的下一週」找日線回測
            wk_after = wk[wk['Date'] > breakout_week]
            if wk_after.empty:
                continue
            retest_week_start = breakout_week
            retest_week_end   = pd.to_datetime(wk_after.iloc[0]['Date'])
            dw = dk[(dk['Date'] > retest_week_start) & (dk['Date'] <= retest_week_end)].copy()
            if dw.empty:
                continue

            # 回測兩種型態 → 觸發（進場日=觸發日；進場價=觸發收盤）
            entry_date = None
            entry_price = None
            trigger_date = None
            retest_mode = None  # 'A_touch_bounce' / 'B_two_day'
            # A) 單日觸碰
            if side == 'long':
                condA = (dw['Low'] <= L * tol_up) & (dw['Close'] > dw['Open'])
            else:
                condA = (dw['High'] >= L * tol_dn) & (dw['Close'] < dw['Open'])
            if condA.any():
                dA = dw[condA].iloc[0]
                trigger_date = dA['Date']
                retest_mode  = 'A_touch_bounce'
                entry_date   = trigger_date
                entry_price  = float(dA['Close'])
            else:
                # B) 兩日形態
                found = False
                for i in range(len(dw) - 1):
                    d1 = dw.iloc[i]
                    d2 = dw.iloc[i+1]
                    if side == 'long':
                        if (d1['Open'] > d1['Close'] and d1['Close'] < L) and (d2['Open'] < d2['Close'] and d2['Close'] > L):
                            trigger_date = d2['Date']
                            retest_mode  = 'B_two_day'
                            entry_date   = trigger_date
                            entry_price  = float(d2['Close'])
                            found = True; break
                    else:
                        if (d1['Open'] < d1['Close'] and d1['Close'] > L) and (d2['Open'] > d2['Close'] and d2['Close'] < L):
                            trigger_date = d2['Date']
                            retest_mode  = 'B_two_day'
                            entry_date   = trigger_date
                            entry_price  = float(d2['Close'])
                            found = True; break
                if not found:
                    continue

            # 停利/停損（停損固定用 L）
            if side == 'long':
                sl_level = L
                tp_price = entry_price * (1.0 + float(tp_pct))
            else:
                sl_level = L
                tp_price = entry_price * (1.0 - float(tp_pct))

            # 3) 出場（以日收盤判斷；從進場日之後開始）
            future_dk = dk[dk['Date'] > entry_date].copy()
            if future_dk.empty:
                continue

            exit_row = None
            exit_reason = None
            for _, r in future_dk.iterrows():
                c = float(r['Close'])
                if side == 'long':
                    if c < sl_level:
                        exit_row = r; exit_reason = 'SL_turn_level_break'; break
                    if c >= tp_price:
                        exit_row = r; exit_reason = 'TP_pct'; break
                else:
                    if c > sl_level:
                        exit_row = r; exit_reason = 'SL_turn_level_break'; break
                    if c <= tp_price:
                        exit_row = r; exit_reason = 'TP_pct'; break

            if exit_row is None:
                exit_row = future_dk.iloc[-1]
                exit_reason = 'FORCED_LAST'

            exit_date  = exit_row['Date']
            exit_price = float(exit_row['Close'])
            pnl     = (exit_price - entry_price) if side == 'long' else (entry_price - exit_price)
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'symbol': sym,
                'turn_week_date': turn_week_date,  # 轉折週
                'turn_type': t['type'],            # 'high' 壓力 / 'low' 支撐
                'turn_price': L,                   # ★ 新定義 L
                'breakout_week': breakout_week,    # 週突破/跌破發生週
                'retest_week_start': retest_week_start,
                'retest_week_end': retest_week_end,
                'retest_mode': retest_mode,        # 'A_touch_bounce' / 'B_two_day'
                'trigger_date': trigger_date,      # 回測觸發日（=進場日）
                'direction': side,                 # 'long' / 'short'
                'entry_date': entry_date,          # 進場日 = 觸發日
                'entry_price': entry_price,        # 進場價 = 觸發價(收盤)
                'tp_pct': float(tp_pct),           # 停利百分比
                'tp_price': tp_price,              # 停利價
                'sl_level': sl_level,              # 停損價 = L
                'exit_date': exit_date,            # 出場日
                'exit_price': exit_price,          # 出場價（收盤）
                'exit_reason': exit_reason,        # 'TP_pct' / 'SL_turn_level_break' / 'FORCED_LAST'
                'pnl': pnl,                        # 損益
                'pnl_pct': pnl_pct,                # 損益(%)
            })

        trades_df = pd.DataFrame(trades).sort_values(['entry_date', 'exit_date']).reset_index(drop=True)
        if not trades_df.empty:
            trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days
            if show_summary:
                n  = len(trades_df)
                wr = (trades_df['pnl'] > 0).mean()
                tot = (1 + trades_df['pnl_pct']/100).prod() - 1
                print(f"[{sym}] Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}  "
                    f"(long={sum(trades_df['direction']=='long')}, short={sum(trades_df['direction']=='short')})")

        return trades_df, levels_df

    def backtest_candle_turn_strategy_v3(
        weekly_csv,
        daily_csv,
        *,
        symbol=None,
        tp_pct: float = 0.03,     # 停利 3%
        retest_tol: float = 0.0,  # 回測容忍(比例)，例 0.001 = 0.1%
        show_summary: bool = False,
        direct_entry_no_retest: bool = True,  # NEW: 不做日線回測，直接進場
        ):
        """
        轉折價 L 的新定義：
        - high 轉折(壓力)：L = min(前週收盤, 本週收盤)
        - low  轉折(支撐)：L = max(前週收盤, 本週收盤)

        其他規則：
        - 預設：僅在突破/跌破週的下一週以日線回測進場（單日觸碰或兩日形態）；
        - 進場日=觸發日、進場價=觸發價(收盤)；停利 tp_pct；停損固定用該筆交易的 L；
        - levels_df：每個轉折的失效週。
        - 若 direct_entry_no_retest=True：突破/跌破後，不做日線回測，
        於下一週「第一個交易日」的收盤直接進場（trigger=entry=該日收盤）。  # NEW
        """
        import pandas as pd
        import numpy as np
        from pathlib import Path
        # ---------- 讀檔 & 排序 ----------
        wk = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        dk = pd.read_csv(daily_csv,  parse_dates=['Date']).sort_values('Date').reset_index(drop=True)

        # ---------- 週轉折（顏色翻轉）+ 新 L 定義 ----------
        def _find_candle_turns(wk_df: pd.DataFrame) -> pd.DataFrame:
            turns = []
            for i in range(1, len(wk_df)):
                prev = wk_df.loc[i-1]
                curr = wk_df.loc[i]
                prev_up   = prev['Close'] > prev['Open']
                prev_down = prev['Close'] < prev['Open']
                curr_up   = curr['Close'] > curr['Open']
                curr_down = curr['Close'] < curr['Open']

                if prev_up and curr_down:
                    L = float(max(prev['Close'], curr['Open']))   # 原有邏輯保留
                    turns.append({'date': curr['Date'], 'type': 'high', 'price': L})
                if prev_down and curr_up:
                    L = float(min(prev['Close'], curr['Open']))   # 原有邏輯保留
                    turns.append({'date': curr['Date'], 'type': 'low',  'price': L})
            return pd.DataFrame(turns, columns=['date','type','price']) if len(turns) else pd.DataFrame(columns=['date','type','price'])

        turns = _find_candle_turns(wk)

        # ---------- 轉折失效表（levels_df） ----------
        def _build_levels_table(wk_df: pd.DataFrame, turns_df: pd.DataFrame) -> pd.DataFrame:
            rows = []
            idx_map = {wk_df.loc[i, 'Date']: i for i in range(len(wk_df))}
            for _, t in turns_df.iterrows():
                turn_week_date = pd.to_datetime(t['date'])
                level_type = 'resistance' if t['type'] == 'high' else 'support'
                L = float(t['price'])
                i0 = idx_map.get(turn_week_date, None)
                invalid_week = pd.NaT
                invalid_reason = ""
                if i0 is not None:
                    for j in range(i0+1, len(wk_df)):
                        c = float(wk_df.loc[j, 'Close'])
                        d = wk_df.loc[j, 'Date']
                        if level_type == 'support':
                            if c < L:
                                invalid_week  = d
                                invalid_reason= 'close_below_support'
                                break
                        else:
                            if c > L:
                                invalid_week  = d
                                invalid_reason= 'close_above_resistance'
                                break
                rows.append({
                    'turn_week_date': turn_week_date,
                    'level_type': level_type,
                    'level_price': L,
                    'invalid_week': invalid_week,
                    'invalid_reason': invalid_reason,
                    'still_valid': pd.isna(invalid_week)
                })
            return pd.DataFrame(rows).sort_values('turn_week_date').reset_index(drop=True)

        levels_df = _build_levels_table(wk, turns)

        # ---------- 推斷 symbol（可選） ----------
        def _infer_symbol(sym, wk_path, dk_path):
            if sym: return str(sym)
            for p in [wk_path, dk_path]:
                name = Path(p).stem
                if name: return name
            return ""
        sym = _infer_symbol(symbol, weekly_csv, daily_csv)

        # ---------- 主交易流程 ----------
        trades = []
        tol_up = 1.0 + float(retest_tol)
        tol_dn = 1.0 - float(retest_tol)

        for _, t in turns.iterrows():
            turn_week_date = pd.to_datetime(t['date'])
            turn_type = t['type']            # 'high'(壓力) / 'low'(支撐)
            L = float(t['price'])

            wk_row = wk[wk['Date'] == turn_week_date]
            if wk_row.empty:
                continue
            idx = wk_row.index[0]
            if idx + 1 >= len(wk):
                continue

            # 1) 找到首次突破/跌破的週
            future_wk = wk.iloc[idx+1:].copy()
            if turn_type == 'high':
                hit_wk = future_wk[future_wk['Close'] > L]
                side = 'long'
            else:
                hit_wk = future_wk[future_wk['Close'] < L]
                side = 'short'
            if hit_wk.empty:
                continue
            breakout_week = pd.to_datetime(hit_wk.iloc[0]['Date'])

            # 2) 取「breakout 週後的下一週」之日線窗
            wk_after = wk[wk['Date'] > breakout_week]
            if wk_after.empty:
                continue
            retest_week_start = breakout_week
            retest_week_end   = pd.to_datetime(wk_after.iloc[0]['Date'])
            dw = dk[(dk['Date'] > retest_week_start) & (dk['Date'] <= retest_week_end)].copy()
            if dw.empty:
                continue

            # ===== NEW: 直接進場模式 =====
            if direct_entry_no_retest:
                d0 = dw.iloc[0]                     # 下一週第一個交易日
                trigger_date = d0['Date']
                retest_mode  = 'NO_RETEST_DIRECT'   # 標示用途
                entry_date   = trigger_date
                entry_price  = float(d0['Close'])   # 以收盤價進場（與原規則一致）
            else:
                # ===== 原本的日線回測模式 =====
                entry_date = None
                entry_price = None
                trigger_date = None
                retest_mode = None  # 'A_touch_bounce' / 'B_two_day'

                # A) 單日觸碰
                if side == 'long':
                    condA = (dw['Low'] <= L * tol_up) & (dw['Close'] > dw['Open'])
                else:
                    condA = (dw['High'] >= L * tol_dn) & (dw['Close'] < dw['Open'])
                if condA.any():
                    dA = dw[condA].iloc[0]
                    trigger_date = dA['Date']
                    retest_mode  = 'A_touch_bounce'
                    entry_date   = trigger_date
                    entry_price  = float(dA['Close'])
                else:
                    # B) 兩日形態
                    found = False
                    for i in range(len(dw) - 1):
                        d1 = dw.iloc[i]
                        d2 = dw.iloc[i+1]
                        if side == 'long':
                            if (d1['Open'] > d1['Close'] and d1['Close'] < L) and (d2['Open'] < d2['Close'] and d2['Close'] > L):
                                trigger_date = d2['Date']; retest_mode = 'B_two_day'
                                entry_date = trigger_date; entry_price = float(d2['Close'])
                                found = True; break
                        else:
                            if (d1['Open'] < d1['Close'] and d1['Close'] > L) and (d2['Open'] > d2['Close'] and d2['Close'] < L):
                                trigger_date = d2['Date']; retest_mode = 'B_two_day'
                                entry_date = trigger_date; entry_price = float(d2['Close'])
                                found = True; break
                    if not found:
                        continue

            # 停利/停損（停損固定用 L）
            if side == 'long':
                sl_level = L
                tp_price = entry_price * (1.0 + float(tp_pct))
            else:
                sl_level = L
                tp_price = entry_price * (1.0 - float(tp_pct))

            # 3) 出場（以日收盤判斷；從進場日之後開始）
            future_dk = dk[dk['Date'] > entry_date].copy()
            if future_dk.empty:
                continue

            exit_row = None
            exit_reason = None
            for _, r in future_dk.iterrows():
                c = float(r['Close'])
                if side == 'long':
                    if c < sl_level:
                        exit_row = r; exit_reason = 'SL_turn_level_break'; break
                    if c >= tp_price:
                        exit_row = r; exit_reason = 'TP_pct'; break
                else:
                    if c > sl_level:
                        exit_row = r; exit_reason = 'SL_turn_level_break'; break
                    if c <= tp_price:
                        exit_row = r; exit_reason = 'TP_pct'; break

            if exit_row is None:
                exit_row = future_dk.iloc[-1]
                exit_reason = 'FORCED_LAST'

            exit_date  = exit_row['Date']
            exit_price = float(exit_row['Close'])
            pnl     = (exit_price - entry_price) if side == 'long' else (entry_price - exit_price)
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'symbol': sym,
                'turn_week_date': turn_week_date,
                'turn_type': t['type'],
                'turn_price': L,
                'breakout_week': breakout_week,
                'retest_week_start': retest_week_start,
                'retest_week_end': retest_week_end,
                'retest_mode': retest_mode,        # 這裡會是 'NO_RETEST_DIRECT' 或原本的兩種
                'trigger_date': trigger_date,
                'direction': side,
                'entry_date': entry_date,
                'entry_price': entry_price,
                'tp_pct': float(tp_pct),
                'tp_price': tp_price,
                'sl_level': sl_level,
                'exit_date': exit_date,
                'exit_price': exit_price,
                'exit_reason': exit_reason,
                'pnl': pnl,
                'pnl_pct': pnl_pct,
            })

        trades_df = pd.DataFrame(trades).sort_values(['entry_date', 'exit_date']).reset_index(drop=True)
        if not trades_df.empty:
            trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days
            if show_summary:
                n  = len(trades_df)
                wr = (trades_df['pnl'] > 0).mean()
                tot = (1 + trades_df['pnl_pct']/100).prod() - 1
                print(f"[{sym}] Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}  "
                    f"(long={sum(trades_df['direction']=='long')}, short={sum(trades_df['direction']=='short')})")

        return trades_df, levels_df

    def backtest_candle_turn_strategy_v6(
            weekly_csv,
            daily_csv,
            *,
            symbol=None,
            tp_pct: float = 0.03,         # 停利 %（當 exit_mode='tp_pct' 時生效）
            retest_tol: float = 0.0,      # 觸碰容忍 (比例)，例 0.001=0.1%
            show_summary: bool = False,
            direct_entry_no_retest: bool = True,
            signal_tf: str = "week",      # 'week' 或 'month'：突破K用的時間框架
            retest_tf: str = "day",       # 'day' 或 'week'：回測K用的時間框架（相容 'daily'/'weekly'）
            monthly_csv = None,           # signal_tf='month' 時可提供月K；未提供則用日線重採樣
            exit_mode: str = "tp_pct",    # ★ 出場模式：'tp_pct' 或 'ma'
            exit_ma_days: int = 20,       # ★ 出場用的日均線長度（exit_mode='ma' 時生效）
            max_gap_weeks: int | None = None,  # ★ 新增：轉折→首次突破/跌破 相隔超過幾週就忽略（None=不篩）
            export_excel_path: str | None = None,  # ★ 新增：若提供路徑，輸出 trades/levels/summary 到一個 Excel
        ):
        """
        出場條件（擇一）：
        - exit_mode='tp_pct' ：價格相對進場價達到 tp_pct 就出場（多頭：>=；空頭：<=）
        - exit_mode='ma'     ：日線對 SMA(exit_ma_days) 發生反向交叉（多頭：跌破；空頭：突破）
                                * 使用「交叉」避免噪音：prev 在均線同側、當日收盤跨到另一側
        其他邏輯：
        - 以 signal_tf（週/月）偵測『顏色翻轉』求轉折價 L（沿用你的 max/min 寫法）
        - 找到首次「收盤突破/跌破 L」的訊號期 → 僅在「下一個訊號期」內用 retest_tf（日/週）找觸發
        - direct_entry_no_retest=True：不回測，於下一期第一根（retest_tf 粒度）收盤進場

        ★ 新增功能：
        - 記錄 gap_weeks：轉折點到首次突破/跌破之間相隔幾週
        * signal_tf='week'：gap_weeks = 訊號K期數差（即週數）
        * signal_tf='month'：gap_weeks = floor((breakout_date - turn_date).days / 7)
        - 以 max_gap_weeks 篩選：若 gap_weeks > max_gap_weeks，忽略該轉折（不產生交易）
        - 匯出 Excel（trades/levels/summary），並在 summary/終端輸出 gap_weeks 統計
        """
        import pandas as pd
        import numpy as np
        from pathlib import Path

        # ---------- 讀入日線（並預先算出 exit 用的 SMA） ----------
        dk = pd.read_csv(daily_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        dk[f'EXIT_SMA{exit_ma_days}']      = dk['Close'].rolling(exit_ma_days, min_periods=exit_ma_days).mean()
        dk['prev_Close']                   = dk['Close'].shift(1)
        dk[f'prev_EXIT_SMA{exit_ma_days}'] = dk[f'EXIT_SMA{exit_ma_days}'].shift(1)

        # ---------- 讀入週/月線作為「訊號週期」 ----------
        signal_tf = (signal_tf or "week").lower()
        if signal_tf not in ("week", "month"):
            raise ValueError("signal_tf 必須為 'week' 或 'month'")

        if signal_tf == "week":
            sig = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        else:
            if monthly_csv:
                sig = pd.read_csv(monthly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            else:
                _d = dk.set_index('Date')
                sig = _d.resample('M', label='right', closed='right').agg({
                    'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
                }).dropna(subset=['Open','High','Low','Close']).reset_index()

        # ---------- 由日線重採樣成周線（供 retest_tf='week' 用） ----------
        wk_from_d = dk.set_index('Date').resample('W-FRI', label='right', closed='right').agg({
            'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
        }).dropna(subset=['Open','High','Low','Close']).reset_index()

        # ---------- 轉折偵測（顏色翻轉 → L） ----------
        def _find_candle_turns(df: pd.DataFrame) -> pd.DataFrame:
            turns = []
            for i in range(1, len(df)):
                prev = df.loc[i-1]
                curr = df.loc[i]
                prev_up   = prev['Close'] > prev['Open']
                prev_down = prev['Close'] < prev['Open']
                curr_up   = curr['Close'] > curr['Open']
                curr_down = curr['Close'] < curr['Open']

                if prev_up and curr_down:
                    L = float(max(prev['Close'], curr['Open']))
                    turns.append({'date': curr['Date'], 'type': 'high', 'price': L})
                if prev_down and curr_up:
                    L = float(min(prev['Close'], curr['Open']))
                    turns.append({'date': curr['Date'], 'type': 'low',  'price': L})
            return pd.DataFrame(turns, columns=['date','type','price']) if len(turns) else pd.DataFrame(columns=['date','type','price'])

        turns = _find_candle_turns(sig)

        # ---------- 失效表（以 signal_tf 收盤判定） ----------
        def _build_levels_table(px: pd.DataFrame, turns_df: pd.DataFrame) -> pd.DataFrame:
            rows = []
            idx_map = {px.loc[i, 'Date']: i for i in range(len(px))}
            for _, t in turns_df.iterrows():
                t_date = pd.to_datetime(t['date'])
                level_type = 'resistance' if t['type']=='high' else 'support'
                L = float(t['price'])
                i0 = idx_map.get(t_date, None)
                invalid_week = pd.NaT
                invalid_reason = ""
                if i0 is not None:
                    for j in range(i0+1, len(px)):
                        c = float(px.loc[j, 'Close'])
                        d = px.loc[j, 'Date']
                        if level_type == 'support':
                            if c < L:
                                invalid_week = d; invalid_reason='close_below_support'; break
                        else:
                            if c > L:
                                invalid_week = d; invalid_reason='close_above_resistance'; break
                rows.append({
                    'turn_week_date': t_date,        # 名稱沿用（即使 signal_tf=month）
                    'level_type': level_type,
                    'level_price': L,
                    'invalid_week': invalid_week,
                    'invalid_reason': invalid_reason,
                    'still_valid': pd.isna(invalid_week)
                })
            return pd.DataFrame(rows).sort_values('turn_week_date').reset_index(drop=True)

        levels_df = _build_levels_table(sig, turns)

        # ---------- 推斷 symbol ----------
        def _infer_symbol(sym, wk_path, dk_path):
            if sym: return str(sym)
            for p in [wk_path, dk_path]:
                try:
                    name = Path(p).stem
                    if name: return name
                except:
                    pass
            return ""
        sym = _infer_symbol(symbol, weekly_csv, daily_csv)

        # ---------- 「下一個訊號期」邊界 + 日/週窗 ----------
        def _next_signal_bounds(i):
            if i + 1 >= len(sig): return None, None
            return sig.loc[i, 'Date'], sig.loc[i+1, 'Date']

        def _days_in_next_period(i):
            b = _next_signal_bounds(i)
            if b == (None, None): return None
            start, end = b
            return dk[(dk['Date'] > start) & (dk['Date'] <= end)].copy()

        def _weeks_in_next_period(i):
            b = _next_signal_bounds(i)
            if b == (None, None): return None
            start, end = b
            return wk_from_d[(wk_from_d['Date'] > start) & (wk_from_d['Date'] <= end)].copy()

        # ---------- 正規化 retest_tf ----------
        _rt = (retest_tf or "day").strip().lower()
        if _rt in ("day", "daily", "d"):
            norm_retest_tf = "day"
        elif _rt in ("week", "weekly", "w"):
            norm_retest_tf = "week"
        else:
            raise ValueError("retest_tf 必須為 'day' 或 'week'（也相容 'daily'/'weekly'）")

        # ---------- 出場邏輯（兩種擇一） ----------
        exit_mode = (exit_mode or "tp_pct").strip().lower()
        if exit_mode not in ("tp_pct", "ma"):
            raise ValueError("exit_mode 必須是 'tp_pct' 或 'ma'")

        def _exit_by_tp(entry_date: pd.Timestamp, side: str, entry_price: float, sl_level: float):
            future = dk[dk['Date'] > entry_date].copy()
            if future.empty: return None
            target = entry_price * (1.0 + float(tp_pct)) if side == 'long' else entry_price * (1.0 - float(tp_pct))

            for _, r in future.iterrows():
                c = float(r['Close'])
                # 先停損、後停利 —— 與 V3 同序
                if side == 'long':
                    if c < sl_level:
                        return (r['Date'], c, 'SL_turn_level_break')
                    if c >= target:
                        return (r['Date'], c, 'TP_pct')
                else:
                    if c > sl_level:
                        return (r['Date'], c, 'SL_turn_level_break')
                    if c <= target:
                        return (r['Date'], c, 'TP_pct')
            r = future.iloc[-1]
            return (r['Date'], float(r['Close']), 'FORCED_LAST')

        def _exit_by_ma(entry_date: pd.Timestamp, side: str, sl_level: float = None):
            col_s  = f'EXIT_SMA{exit_ma_days}'
            col_ps = f'prev_EXIT_SMA{exit_ma_days}'
            future = dk[dk['Date'] > entry_date].copy()
            if future.empty: return None

            for _, r in future.iterrows():
                c  = float(r['Close'])
                pc = float(r['prev_Close']) if not np.isnan(r['prev_Close']) else None
                s  = float(r[col_s])  if not np.isnan(r[col_s])  else None
                ps = float(r[col_ps]) if not np.isnan(r[col_ps]) else None
                if sl_level is not None:
                    if (side == 'long' and c < sl_level) or (side == 'short' and c > sl_level):
                        return (r['Date'], c, 'SL_turn_level_break')
                if s is None or ps is None or pc is None:
                    continue
                if side == 'long':
                    if (pc >= ps) and (c < s):
                        return (r['Date'], c, f'MA{exit_ma_days}_cross')
                else:
                    if (pc <= ps) and (c > s):
                        return (r['Date'], c, f'MA{exit_ma_days}_cross')
            r = future.iloc[-1]
            return (r['Date'], float(r['Close']), 'FORCED_LAST')

        # ---------- 主流程 ----------
        trades = []
        tol_up = 1.0 + float(retest_tol)
        tol_dn = 1.0 - float(retest_tol)

        for _, t in turns.iterrows():
            turn_period_end = pd.to_datetime(t['date'])
            turn_type = t['type']          # 'high' / 'low'
            L = float(t['price'])

            sig_row = sig[sig['Date'] == turn_period_end]
            if sig_row.empty: 
                continue
            idx = sig_row.index[0]
            if idx + 1 >= len(sig): 
                continue

            future_sig = sig.iloc[idx+1:].copy()
            if turn_type == 'high':
                hit_sig = future_sig[future_sig['Close'] > L]; side='long'
            else:
                hit_sig = future_sig[future_sig['Close'] < L]; side='short'
            if hit_sig.empty: 
                continue

            breakout_period_end = pd.to_datetime(hit_sig.iloc[0]['Date'])
            breakout_idx = hit_sig.index[0]

            if signal_tf == 'week':
                gap_weeks = int(breakout_idx - idx)  # 期數差 = 週數
            else:
                gap_weeks = int((breakout_period_end - turn_period_end).days // 7)

            if (max_gap_weeks is not None) and (gap_weeks > int(max_gap_weeks)):
                continue

            window = (_days_in_next_period(hit_sig.index[0]) if norm_retest_tf=='day'
                    else _weeks_in_next_period(hit_sig.index[0]))
            if window is None or window.empty: 
                continue

            if direct_entry_no_retest:
                d0 = window.iloc[0]
                trigger_date = d0['Date']
                retest_mode  = 'NO_RETEST_DIRECT'
                entry_date   = trigger_date
                entry_price  = float(d0['Close'])
            else:
                entry_date = None
                entry_price = None
                trigger_date = None
                if norm_retest_tf == 'day':
                    if side == 'long':
                        condA = (window['Low']  <= L * tol_up) & (window['Close'] > window['Open']) & (window['Close'] > L)
                    else:
                        condA = (window['High'] >= L * tol_dn) & (window['Close'] < window['Open']) & (window['Close'] < L)
                    if condA.any():
                        r = window[condA].iloc[0]
                        trigger_date = r['Date']; entry_date = r['Date']; entry_price = float(r['Close'])
                        retest_mode = 'A_touch_bounce'
                    else:
                        found = False
                        for i2 in range(len(window)-1):
                            d1 = window.iloc[i2]; d2 = window.iloc[i2+1]
                            if side == 'long':
                                if (d1['Open'] > d1['Close'] and d1['Close'] < L) and (d2['Open'] < d2['Close'] and d2['Close'] > L):
                                    trigger_date = d2['Date']; entry_date = d2['Date']; entry_price = float(d2['Close'])
                                    retest_mode = 'B_two_day'; found = True; break
                            else:
                                if (d1['Open'] < d1['Close'] and d1['Close'] > L) and (d2['Open'] > d2['Close'] and d2['Close'] < L):
                                    trigger_date = d2['Date']; entry_date = d2['Date']; entry_price = float(d2['Close'])
                                    retest_mode = 'B_two_day'; found = True; break
                        if not found:
                            continue
                else:
                    if side == 'long':
                        condW = (window['Low']  <= L * tol_up) & (window['Close'] > window['Open']) & (window['Close'] > L)
                    else:
                        condW = (window['High'] >= L * tol_dn) & (window['Close'] < window['Open']) & (window['Close'] < L)
                    ok = window[condW]
                    if ok.empty: 
                        continue
                    r = ok.iloc[0]
                    trigger_date = r['Date']; entry_date = r['Date']; entry_price = float(r['Close'])
                    retest_mode = 'W_touch_bounce'

            if exit_mode == 'tp_pct':
                exit_pack = _exit_by_tp(entry_date, side, entry_price, L)
            else:
                exit_pack = _exit_by_ma(entry_date, side, sl_level=L)
            if exit_pack is None:
                continue
            exit_date, exit_price, exit_reason = exit_pack

            pnl     = (exit_price - entry_price) if side == 'long' else (entry_price - exit_price)
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'symbol': sym,
                'turn_week_date': turn_period_end,
                'turn_type': t['type'],
                'turn_price': L,
                'breakout_week': breakout_period_end,
                'gap_weeks_from_turn_to_breakout': int(gap_weeks),  # ★ 新增欄位
                'retest_tf': norm_retest_tf,
                'retest_mode': 'NO_RETEST_DIRECT' if direct_entry_no_retest else retest_mode,
                'trigger_date': trigger_date,
                'direction': side,
                'entry_date': entry_date,
                'entry_price': float(entry_price),
                'exit_mode': exit_mode,
                'exit_ma_days': (exit_ma_days if exit_mode=='ma' else None),
                'tp_pct': (float(tp_pct) if exit_mode=='tp_pct' else None),
                'exit_date': exit_date,
                'exit_price': float(exit_price),
                'exit_reason': exit_reason,
                'pnl': float(pnl),
                'pnl_pct': float(pnl_pct),
            })

        # ---------- 收尾 ----------
        trades_df = pd.DataFrame(trades)
        if trades_df.empty:
            if show_summary:
                print(f"[{sym}] No trades generated. (levels: {len(levels_df)})")
            # 即使無交易，也可選擇輸出 levels/summary（此處簡化為只回傳）
            return trades_df, levels_df

        trades_df = trades_df.sort_values(['entry_date','exit_date']).reset_index(drop=True)
        trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days

        # === 新增：gap_weeks 的統計摘要 ===
        gap_col = 'gap_weeks_from_turn_to_breakout'
        gap_stats = {}
        try:
            g = trades_df[gap_col].dropna().astype(int)
            if len(g) > 0:
                gap_stats = {
                    'count': int(g.count()),
                    'min': int(g.min()),
                    'max': int(g.max()),
                    'mean': float(g.mean()),
                    'median': float(g.median()),
                }
            else:
                gap_stats = {'count': 0, 'min': None, 'max': None, 'mean': None, 'median': None}
        except Exception:
            gap_stats = {'count': 0, 'min': None, 'max': None, 'mean': None, 'median': None}

        # 分佈區間（可依需求調整）
        def _bucket(x: int):
            if x <= 2: return '0-2'
            if x <= 4: return '3-4'
            if x <= 8: return '5-8'
            return '9+'
        try:
            dist = trades_df[gap_col].dropna().astype(int).map(_bucket).value_counts().to_dict()
        except Exception:
            dist = {}

        # 總績效
        n_trades = len(trades_df)
        win_rate = (trades_df['pnl'] > 0).mean()
        total_ret = (1 + trades_df['pnl_pct']/100).prod() - 1
        long_n = int((trades_df['direction']=='long').sum())
        short_n = n_trades - long_n

        # 額外：依勝敗的 gap 平均
        try:
            gap_by_win = trades_df.assign(win=(trades_df['pnl']>0)).groupby('win')[gap_col].mean().to_dict()
        except Exception:
            gap_by_win = {}

        if show_summary:
            print(
                f"[{sym}] Trades: {n_trades}, WinRate: {win_rate:.2%}, TotalRet: {total_ret:.2%}  "
                f"(long={long_n}, short={short_n})"
            )
            print(f"gap_weeks: count={gap_stats['count']}, min={gap_stats['min']}, "
                f"median={gap_stats['median']}, mean={gap_stats['mean']:.2f} if not None else None, "
                f"max={gap_stats['max']}")
            if dist:
                print(f"gap buckets: {dist}")
            if gap_by_win:
                print(f"avg gap by win/loss: {gap_by_win}")

        # === 新增：輸出 Excel（trades/levels/summary 三工作表） ===
        if export_excel_path:
            summary_rows = [
                {'metric': 'symbol', 'value': sym},
                {'metric': 'trades', 'value': n_trades},
                {'metric': 'win_rate', 'value': f"{win_rate:.4f}"},
                {'metric': 'total_ret', 'value': f"{total_ret:.6f}"},
                {'metric': 'long_trades', 'value': long_n},
                {'metric': 'short_trades', 'value': short_n},
                {'metric': 'max_gap_weeks_filter', 'value': max_gap_weeks},
                {'metric': 'gap_count', 'value': gap_stats.get('count')},
                {'metric': 'gap_min', 'value': gap_stats.get('min')},
                {'metric': 'gap_median', 'value': gap_stats.get('median')},
                {'metric': 'gap_mean', 'value': gap_stats.get('mean')},
                {'metric': 'gap_max', 'value': gap_stats.get('max')},
            ]
            # 把分佈也攤平存進 summary
            for k, v in dist.items():
                summary_rows.append({'metric': f'gap_bucket_{k}', 'value': v})
            # 勝敗各自的平均 gap
            for k, v in gap_by_win.items():
                label = 'win_true' if k is True else 'win_false'
                summary_rows.append({'metric': f'avg_gap_{label}', 'value': v})

            summary_df = pd.DataFrame(summary_rows, columns=['metric','value'])

            # 寫入一個檔案三個 sheet
            with pd.ExcelWriter(export_excel_path) as xw:
                trades_df.to_excel(xw, index=False, sheet_name='trades')
                levels_df.to_excel(xw, index=False, sheet_name='levels')
                summary_df.to_excel(xw, index=False, sheet_name='summary')

        return trades_df, levels_df


        def backtest_candle_turn_strategy_v5(
            weekly_csv,
            daily_csv,
            *,
            symbol=None,
            tp_pct: float = 0.03,         # 停利 %（當 exit_mode='tp_pct' 時生效）
            retest_tol: float = 0.0,      # 觸碰容忍 (比例)，例 0.001=0.1%
            show_summary: bool = False,
            direct_entry_no_retest: bool = True,
            signal_tf: str = "week",      # 'week' 或 'month'：突破K用的時間框架
            retest_tf: str = "day",       # 'day' 或 'week'：回測K用的時間框架（相容 'daily'/'weekly'）
            monthly_csv = None,           # signal_tf='month' 時可提供月K；未提供則用日線重採樣
            exit_mode: str = "tp_pct",    # ★ 出場模式：'tp_pct' 或 'ma'
            exit_ma_days: int = 20,       # ★ 出場用的日均線長度（exit_mode='ma' 時生效）
            ):
            """
            出場條件（擇一）：
            - exit_mode='tp_pct' ：價格相對進場價達到 tp_pct 就出場（多頭：>=；空頭：<=）
            - exit_mode='ma'     ：日線對 SMA(exit_ma_days) 發生反向交叉（多頭：跌破；空頭：突破）
                                    * 使用「交叉」避免噪音：prev 在均線同側、當日收盤跨到另一側
            其他邏輯：
            - 以 signal_tf（週/月）偵測『顏色翻轉』求轉折價 L（沿用你的 max/min 寫法）
            - 找到首次「收盤突破/跌破 L」的訊號期 → 僅在「下一個訊號期」內用 retest_tf（日/週）找觸發
            - direct_entry_no_retest=True：不回測，於下一期第一根（retest_tf 粒度）收盤進場
            """
            import pandas as pd
            import numpy as np
            from pathlib import Path

            # ---------- 讀入日線（並預先算出 exit 用的 SMA） ----------
            dk = pd.read_csv(daily_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            # 出場若選 ma，需要這條均線與上一日值
            dk[f'EXIT_SMA{exit_ma_days}']      = dk['Close'].rolling(exit_ma_days, min_periods=exit_ma_days).mean()
            dk['prev_Close']                   = dk['Close'].shift(1)
            dk[f'prev_EXIT_SMA{exit_ma_days}'] = dk[f'EXIT_SMA{exit_ma_days}'].shift(1)

            # ---------- 讀入週/月線作為「訊號週期」 ----------
            signal_tf = (signal_tf or "week").lower()
            if signal_tf not in ("week", "month"):
                raise ValueError("signal_tf 必須為 'week' 或 'month'")

            if signal_tf == "week":
                sig = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            else:
                if monthly_csv:
                    sig = pd.read_csv(monthly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
                else:
                    _d = dk.set_index('Date')
                    sig = _d.resample('M', label='right', closed='right').agg({
                        'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
                    }).dropna(subset=['Open','High','Low','Close']).reset_index()

            # ---------- 由日線重採樣成周線（供 retest_tf='week' 用） ----------
            wk_from_d = dk.set_index('Date').resample('W-FRI', label='right', closed='right').agg({
                'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
            }).dropna(subset=['Open','High','Low','Close']).reset_index()

            # ---------- 轉折偵測（顏色翻轉 → L） ----------
            def _find_candle_turns(df: pd.DataFrame) -> pd.DataFrame:
                turns = []
                for i in range(1, len(df)):
                    prev = df.loc[i-1]
                    curr = df.loc[i]
                    prev_up   = prev['Close'] > prev['Open']
                    prev_down = prev['Close'] < prev['Open']
                    curr_up   = curr['Close'] > curr['Open']
                    curr_down = curr['Close'] < curr['Open']

                    if prev_up and curr_down:
                        # high 轉折 → 壓力；沿用你原程式的寫法：max(prev.Close, curr.Open)
                        L = float(max(prev['Close'], curr['Open']))
                        turns.append({'date': curr['Date'], 'type': 'high', 'price': L})
                    if prev_down and curr_up:
                        # low 轉折 → 支撐；沿用：min(prev.Close, curr.Open)
                        L = float(min(prev['Close'], curr['Open']))
                        turns.append({'date': curr['Date'], 'type': 'low',  'price': L})
                return pd.DataFrame(turns, columns=['date','type','price']) if len(turns) else pd.DataFrame(columns=['date','type','price'])

            turns = _find_candle_turns(sig)

            # ---------- 失效表（以 signal_tf 收盤判定） ----------
            def _build_levels_table(px: pd.DataFrame, turns_df: pd.DataFrame) -> pd.DataFrame:
                rows = []
                idx_map = {px.loc[i, 'Date']: i for i in range(len(px))}
                for _, t in turns_df.iterrows():
                    t_date = pd.to_datetime(t['date'])
                    level_type = 'resistance' if t['type']=='high' else 'support'
                    L = float(t['price'])
                    i0 = idx_map.get(t_date, None)
                    invalid_week = pd.NaT
                    invalid_reason = ""
                    if i0 is not None:
                        for j in range(i0+1, len(px)):
                            c = float(px.loc[j, 'Close'])
                            d = px.loc[j, 'Date']
                            if level_type == 'support':
                                if c < L:
                                    invalid_week = d; invalid_reason='close_below_support'; break
                            else:
                                if c > L:
                                    invalid_week = d; invalid_reason='close_above_resistance'; break
                    rows.append({
                        'turn_week_date': t_date,        # 名稱沿用（即使 signal_tf=month）
                        'level_type': level_type,
                        'level_price': L,
                        'invalid_week': invalid_week,
                        'invalid_reason': invalid_reason,
                        'still_valid': pd.isna(invalid_week)
                    })
                return pd.DataFrame(rows).sort_values('turn_week_date').reset_index(drop=True)

            levels_df = _build_levels_table(sig, turns)

            # ---------- 推斷 symbol ----------
            def _infer_symbol(sym, wk_path, dk_path):
                if sym: return str(sym)
                for p in [wk_path, dk_path]:
                    try:
                        name = Path(p).stem
                        if name: return name
                    except:
                        pass
                return ""
            sym = _infer_symbol(symbol, weekly_csv, daily_csv)

            # ---------- 「下一個訊號期」邊界 + 日/週窗 ----------
            def _next_signal_bounds(i):
                if i + 1 >= len(sig): return None, None
                return sig.loc[i, 'Date'], sig.loc[i+1, 'Date']

            def _days_in_next_period(i):
                b = _next_signal_bounds(i)
                if b == (None, None): return None
                start, end = b
                return dk[(dk['Date'] > start) & (dk['Date'] <= end)].copy()

            def _weeks_in_next_period(i):
                b = _next_signal_bounds(i)
                if b == (None, None): return None
                start, end = b
                return wk_from_d[(wk_from_d['Date'] > start) & (wk_from_d['Date'] <= end)].copy()

            # ---------- 正規化 retest_tf ----------
            _rt = (retest_tf or "day").strip().lower()
            if _rt in ("day", "daily", "d"):
                norm_retest_tf = "day"
            elif _rt in ("week", "weekly", "w"):
                norm_retest_tf = "week"
            else:
                raise ValueError("retest_tf 必須為 'day' 或 'week'（也相容 'daily'/'weekly'）")

            # ---------- 出場邏輯（兩種擇一） ----------
            exit_mode = (exit_mode or "tp_pct").strip().lower()
            if exit_mode not in ("tp_pct", "ma"):
                raise ValueError("exit_mode 必須是 'tp_pct' 或 'ma'")

            def _exit_by_tp(entry_date: pd.Timestamp, side: str, entry_price: float, sl_level: float):
                future = dk[dk['Date'] > entry_date].copy()
                if future.empty: return None
                target = entry_price * (1.0 + float(tp_pct)) if side == 'long' else entry_price * (1.0 - float(tp_pct))

                for _, r in future.iterrows():
                    c = float(r['Close'])
                    # 先停損、後停利 —— 與 V3 同序
                    if side == 'long':
                        if c < sl_level:              # 嚴格 < 與 V3 一致
                            return (r['Date'], c, 'SL_turn_level_break')
                        if c >= target:
                            return (r['Date'], c, 'TP_pct')
                    else:
                        if c > sl_level:
                            return (r['Date'], c, 'SL_turn_level_break')
                        if c <= target:
                            return (r['Date'], c, 'TP_pct')
                r = future.iloc[-1]
                return (r['Date'], float(r['Close']), 'FORCED_LAST')

            def _exit_by_ma(entry_date: pd.Timestamp, side: str, sl_level: float = None):
                col_s  = f'EXIT_SMA{exit_ma_days}'
                col_ps = f'prev_EXIT_SMA{exit_ma_days}'
                future = dk[dk['Date'] > entry_date].copy()
                if future.empty: return None

                # 使用「反向交叉」：前一日在同側、今日跨到對側
                for _, r in future.iterrows():
                    c  = float(r['Close'])
                    pc = float(r['prev_Close']) if not np.isnan(r['prev_Close']) else None
                    s  = float(r[col_s])  if not np.isnan(r[col_s])  else None
                    ps = float(r[col_ps]) if not np.isnan(r[col_ps]) else None
                    # 先硬停損：用 L
                    if sl_level is not None:
                        if (side == 'long' and c < sl_level) or (side == 'short' and c > sl_level):
                            return (r['Date'], c, 'SL_turn_level_break')
                    if s is None or ps is None or pc is None:
                        continue
                    if side == 'long':
                        if (pc >= ps) and (c < s):
                            return (r['Date'], c, f'MA{exit_ma_days}_cross')
                    else:
                        if (pc <= ps) and (c > s):
                            return (r['Date'], c, f'MA{exit_ma_days}_cross')
                # 沒觸發 → 最後一天強制出場
                r = future.iloc[-1]
                return (r['Date'], float(r['Close']), 'FORCED_LAST')

            # ---------- 主流程 ----------
            trades = []
            tol_up = 1.0 + float(retest_tol)
            tol_dn = 1.0 - float(retest_tol)

            for _, t in turns.iterrows():
                turn_period_end = pd.to_datetime(t['date'])
                turn_type = t['type']          # 'high' / 'low'
                L = float(t['price'])

                # 找轉折期索引
                sig_row = sig[sig['Date'] == turn_period_end]
                if sig_row.empty: continue
                idx = sig_row.index[0]
                if idx + 1 >= len(sig): continue

                # 1) 在 signal_tf 後續期中找「首次突破/跌破 L」的訊號期
                future_sig = sig.iloc[idx+1:].copy()
                if turn_type == 'high':
                    hit_sig = future_sig[future_sig['Close'] > L]; side='long'
                else:
                    hit_sig = future_sig[future_sig['Close'] < L]; side='short'
                if hit_sig.empty: continue
                breakout_period_end = pd.to_datetime(hit_sig.iloc[0]['Date'])

                # 2) 僅在「breakout 期之後的下一個 signal 期」尋找回測（依 norm_retest_tf 選日/週）
                window = (_days_in_next_period(hit_sig.index[0]) if norm_retest_tf=='day'
                        else _weeks_in_next_period(hit_sig.index[0]))
                if window is None or window.empty: continue

                # === 進場 ===
                if direct_entry_no_retest:
                    d0 = window.iloc[0]
                    trigger_date = d0['Date']
                    retest_mode  = 'NO_RETEST_DIRECT'
                    entry_date   = trigger_date
                    entry_price  = float(d0['Close'])   # 與你的 v3 一致：用收盤進場
                else:
                    entry_date = None
                    entry_price = None
                    trigger_date = None
                    if norm_retest_tf == 'day':
                        # A) 單日觸碰
                        if side == 'long':
                            condA = (window['Low']  <= L * tol_up) & (window['Close'] > window['Open'])
                        else:
                            condA = (window['High'] >= L * tol_dn) & (window['Close'] < window['Open'])
                        if condA.any():
                            r = window[condA].iloc[0]
                            trigger_date = r['Date']; entry_date = r['Date']; entry_price = float(r['Close'])
                            retest_mode = 'A_touch_bounce'
                        else:
                            # B) 兩日形態
                            found = False
                            for i2 in range(len(window)-1):
                                d1 = window.iloc[i2]; d2 = window.iloc[i2+1]
                                if side == 'long':
                                    if (d1['Open'] > d1['Close'] and d1['Close'] < L) and (d2['Open'] < d2['Close'] and d2['Close'] > L):
                                        trigger_date = d2['Date']; entry_date = d2['Date']; entry_price = float(d2['Close'])
                                        retest_mode = 'B_two_day'; found = True; break
                                else:
                                    if (d1['Open'] < d1['Close'] and d1['Close'] > L) and (d2['Open'] > d2['Close'] and d2['Close'] < L):
                                        trigger_date = d2['Date']; entry_date = d2['Date']; entry_price = float(d2['Close'])
                                        retest_mode = 'B_two_day'; found = True; break
                            if not found:
                                continue
                    else:
                        # week：單周觸碰/轉向
                        if side == 'long':
                            condW = (window['Low']  <= L * tol_up) & (window['Close'] > window['Open'])
                        else:
                            condW = (window['High'] >= L * tol_dn) & (window['Close'] < window['Open'])
                        ok = window[condW]
                        if ok.empty: continue
                        r = ok.iloc[0]
                        trigger_date = r['Date']; entry_date = r['Date']; entry_price = float(r['Close'])
                        retest_mode = 'W_touch_bounce'

                # 3) 出場（依 exit_mode 擇一）
                if exit_mode == 'tp_pct':
                    exit_pack = _exit_by_tp(entry_date, side, entry_price,L)
                else:
                    exit_pack = _exit_by_ma(entry_date, side, sl_level=L)
                if exit_pack is None:
                    continue
                exit_date, exit_price, exit_reason = exit_pack

                pnl     = (exit_price - entry_price) if side == 'long' else (entry_price - exit_price)
                pnl_pct = (pnl / entry_price) * 100.0

                trades.append({
                    'symbol': sym,
                    'turn_week_date': turn_period_end,   # 名稱沿用（即使 signal_tf=month）
                    'turn_type': t['type'],
                    'turn_price': L,
                    'breakout_week': breakout_period_end,
                    'retest_tf': norm_retest_tf,         # 'day' / 'week'
                    'retest_mode': 'NO_RETEST_DIRECT' if direct_entry_no_retest else retest_mode,
                    'trigger_date': trigger_date,
                    'direction': side,
                    'entry_date': entry_date,
                    'entry_price': float(entry_price),
                    'exit_mode': exit_mode,              # ★ 紀錄出場模式
                    'exit_ma_days': (exit_ma_days if exit_mode=='ma' else None),
                    'tp_pct': (float(tp_pct) if exit_mode=='tp_pct' else None),
                    'exit_date': exit_date,
                    'exit_price': float(exit_price),
                    'exit_reason': exit_reason,
                    'pnl': float(pnl),
                    'pnl_pct': float(pnl_pct),
                })

            # ---------- 收尾 ----------
            trades_df = pd.DataFrame(trades)
            if trades_df.empty:
                if show_summary:
                    print(f"[{sym}] No trades generated. (levels: {len(levels_df)})")
                return trades_df, levels_df

            trades_df = trades_df.sort_values(['entry_date','exit_date']).reset_index(drop=True)
            trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days

            if show_summary:
                n  = len(trades_df)
                wr = (trades_df['pnl'] > 0).mean()
                tot = (1 + trades_df['pnl_pct']/100).prod() - 1
                print(f"[{sym}] Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}  "
                    f"(long={sum(trades_df['direction']=='long')}, short={sum(trades_df['direction']=='short')})")

            return trades_df, levels_df


        def backtest_daily_turn_at_weekly_level_v1(
        weekly_csv,
        daily_csv,
        *,
        symbol=None,
        tp_pct: float = 0.03,        # 停利%
        hit_tol: float = 0.001,      # 觸及容忍(比例)：例 0.001 = 0.1%
        one_trade_per_level: bool = True,
        show_summary: bool = False,
        ):
            """
            策略：日線轉折出現在週支撐/壓力附近即進場
            - 多頭：日線「低轉折」（前日收跌、當日收漲） + 當日K觸及「週支撐 L±tol」
            - 空頭：日線「高轉折」（前日收漲、當日收跌） + 當日K觸及「週壓力 L±tol」
            - 停利：tp_pct
            - 停損：多頭→日收盤 < 支撐 L；空頭→日收盤 > 壓力 L

            輸入：
            weekly_csv : 需含 ['Date','Open','High','Low','Close']（週資料）
            daily_csv  : 需含 ['Date','Open','High','Low','Close']（日資料）

            輸出：
            trades_df, levels_df
            """
            import pandas as pd
            import numpy as np
            from pathlib import Path

            # ---------- 讀檔 & 排序 ----------
            wk = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            dk = pd.read_csv(daily_csv,  parse_dates=['Date']).sort_values('Date').reset_index(drop=True)

            # ---------- 推斷 symbol（可選） ----------
            def _infer_symbol(sym, wk_path, dk_path):
                if sym: return str(sym)
                for p in [wk_path, dk_path]:
                    name = Path(p).stem
                    if name: return name
                return ""
            sym = _infer_symbol(symbol, weekly_csv, daily_csv)

            # ---------- 週轉折 + L 定義（延續你上一版的寫法） ----------
            def _find_weekly_turns_and_levels(wk_df: pd.DataFrame) -> pd.DataFrame:
                """
                prev_up & curr_down → high 轉折（壓力）
                prev_down & curr_up → low  轉折（支撐）

                ★ L 定義沿用你上一版（注意：用 prev['Close'] 與 curr['Open'] 的組合）
                - high：L = max(prev['Close'], curr['Open'])
                - low ：L = min(prev['Close'], curr['Open'])
                """
                turns = []
                for i in range(1, len(wk_df)):
                    prev = wk_df.loc[i-1]
                    curr = wk_df.loc[i]
                    prev_up   = prev['Close'] > prev['Open']
                    prev_down = prev['Close'] < prev['Open']
                    curr_up   = curr['Close'] > curr['Open']
                    curr_down = curr['Close'] < curr['Open']

                    if prev_up and curr_down:
                        L = float(max(prev['Close'], curr['Open']))  # 壓力
                        turns.append({'turn_week_date': curr['Date'], 'level_type': 'resistance', 'level_price': L})
                    if prev_down and curr_up:
                        L = float(min(prev['Close'], curr['Open']))  # 支撐
                        turns.append({'turn_week_date': curr['Date'], 'level_type': 'support',    'level_price': L})
                return pd.DataFrame(turns, columns=['turn_week_date','level_type','level_price'])

            turns = _find_weekly_turns_and_levels(wk)
            if turns.empty:
                return pd.DataFrame(columns=[]), pd.DataFrame(columns=[])

            # ---------- 週等級失效偵測（用週收盤判斷，供觀察/限制交易窗） ----------
            def _mark_weekly_invalid(wk_df: pd.DataFrame, levels_df: pd.DataFrame) -> pd.DataFrame:
                idx_map = {wk_df.loc[i, 'Date']: i for i in range(len(wk_df))}
                rows = []
                for _, r in levels_df.iterrows():
                    tdate = pd.to_datetime(r['turn_week_date'])
                    Ltype = r['level_type']
                    L     = float(r['level_price'])
                    i0 = idx_map.get(tdate, None)
                    invalid_week = pd.NaT
                    invalid_reason = ""
                    if i0 is not None:
                        for j in range(i0+1, len(wk_df)):
                            c = float(wk_df.loc[j, 'Close'])
                            d = wk_df.loc[j, 'Date']
                            if Ltype == 'support':
                                if c < L:  # 週收盤跌破支撐
                                    invalid_week = d; invalid_reason = 'wk_close_below_support'; break
                            else:
                                if c > L:  # 週收盤突破壓力
                                    invalid_week = d; invalid_reason = 'wk_close_above_resistance'; break
                    rows.append({
                        'turn_week_date': tdate,
                        'level_type': Ltype,
                        'level_price': L,
                        'invalid_week': invalid_week,
                        'invalid_reason': invalid_reason,
                        'still_valid': pd.isna(invalid_week)
                    })
                return pd.DataFrame(rows).sort_values('turn_week_date').reset_index(drop=True)

            levels_df = _mark_weekly_invalid(wk, turns)

            # ---------- 日線轉折（兩日顏色翻轉） ----------
            def _find_daily_turns(dk_df: pd.DataFrame) -> pd.DataFrame:
                """
                prev_up & curr_down → 'high' 轉折（日線高點轉折）
                prev_down & curr_up → 'low'  轉折（日線低點轉折）
                """
                out = []
                for i in range(1, len(dk_df)):
                    prev = dk_df.iloc[i-1]
                    curr = dk_df.iloc[i]
                    prev_up   = prev['Close'] > prev['Open']
                    prev_down = prev['Close'] < prev['Open']
                    curr_up   = curr['Close'] > curr['Open']
                    curr_down = curr['Close'] < curr['Open']
                    if prev_up and curr_down:
                        out.append({'date': curr['Date'], 'type': 'high', 'Open': curr['Open'], 'High': curr['High'], 'Low': curr['Low'], 'Close': curr['Close']})
                    elif prev_down and curr_up:
                        out.append({'date': curr['Date'], 'type': 'low',  'Open': curr['Open'], 'High': curr['High'], 'Low': curr['Low'], 'Close': curr['Close']})
                return pd.DataFrame(out)

            dturns = _find_daily_turns(dk)

            # ---------- 工具：判定「當日K 是否觸及 L±tol 範圍」 ----------
            def _in_band(day_row, L, tol):
                band_low  = L * (1 - tol)
                band_high = L * (1 + tol)
                # 當日區間與帶狀是否重疊
                return not (day_row['High'] < band_low or day_row['Low'] > band_high)

            # ---------- 主交易流程 ----------
            trades = []
            # 快速索引：每個週期的下一週起訖（用來限制不在週失效後繼續找訊號）
            wk_dates = wk['Date'].tolist()
            wk_next_map = {wk_dates[i]: wk_dates[i+1] if i+1 < len(wk_dates) else pd.Timestamp.max for i in range(len(wk_dates))}

            for lvl_id, lvl in levels_df.reset_index().iterrows():
                turn_week_date = pd.to_datetime(lvl['turn_week_date'])
                Ltype = lvl['level_type']          # 'support' / 'resistance'
                L     = float(lvl['level_price'])
                # 此等級有效的日線搜尋窗：自「轉折週之後」開始，到「週失效」為止
                wk_start = turn_week_date
                wk_end   = lvl['invalid_week'] if pd.notna(lvl['invalid_week']) else pd.Timestamp.max

                # 在此窗內找日線轉折
                dw = dk[(dk['Date'] > wk_start) & (dk['Date'] < wk_end)].copy()
                if dw.empty or dturns.empty:
                    continue

                # 把日轉折限制在同一窗內，並配對支撐/壓力
                dturns_win = dturns[(dturns['date'] > wk_start) & (dturns['date'] < wk_end)].copy()
                if dturns_win.empty:
                    continue

                # 按照條件配對
                # 多：日線 'low' 轉折 + 觸及支撐
                # 空：日線 'high' 轉折 + 觸及壓力
                need_type = 'low' if Ltype == 'support' else 'high'
                cand = dturns_win[dturns_win['type'] == need_type].copy()
                if cand.empty:
                    continue

                taken_for_this_level = False
                for _, drow in cand.iterrows():
                    # 取該日完整K棒
                    day = dw[dw['Date'] == drow['date']]
                    if day.empty:
                        continue
                    day = day.iloc[0]

                    # 當日K是否觸及 L±hit_tol
                    if not _in_band(day, L, hit_tol):
                        continue

                    # ===== 成交：用「當日收盤」進場 =====
                    direction = 'long' if Ltype == 'support' else 'short'
                    entry_date  = drow['date']
                    entry_price = float(day['Close'])
                    sl_level    = L
                    tp_price    = entry_price * (1.0 + tp_pct) if direction == 'long' else entry_price * (1.0 - tp_pct)

                    # 從下一日開始往後找出場
                    future = dk[dk['Date'] > entry_date].copy()
                    if future.empty:
                        continue

                    exit_row = None
                    exit_reason = None
                    for _, r in future.iterrows():
                        c = float(r['Close'])
                        if direction == 'long':
                            if c < sl_level:
                                exit_row = r; exit_reason = 'SL_support_broken'; break
                            if c >= tp_price:
                                exit_row = r; exit_reason = 'TP_pct'; break
                        else:
                            if c > sl_level:
                                exit_row = r; exit_reason = 'SL_resistance_broken'; break
                            if c <= tp_price:
                                exit_row = r; exit_reason = 'TP_pct'; break

                    if exit_row is None:
                        exit_row = future.iloc[-1]
                        exit_reason = 'FORCED_LAST'

                    exit_date  = exit_row['Date']
                    exit_price = float(exit_row['Close'])
                    pnl        = (exit_price - entry_price) if direction == 'long' else (entry_price - exit_price)
                    pnl_pct    = (pnl / entry_price) * 100.0

                    trades.append({
                        'symbol': sym,
                        'anchor_level_id': int(lvl_id),
                        'turn_week_date': turn_week_date,
                        'level_type': Ltype,          # 'support' / 'resistance'
                        'level_price': L,
                        'signal_date': entry_date,    # 日線轉折發生日（也是進場日）
                        'signal_type': need_type,     # 'low' or 'high'
                        'direction': direction,       # 'long' / 'short'
                        'entry_date': entry_date,
                        'entry_price': entry_price,
                        'tp_pct': float(tp_pct),
                        'tp_price': tp_price,
                        'sl_level': sl_level,
                        'exit_date': exit_date,
                        'exit_price': exit_price,
                        'exit_reason': exit_reason,
                        'pnl': pnl,
                        'pnl_pct': pnl_pct,
                    })

                    if one_trade_per_level:
                        taken_for_this_level = True
                        break

                # 若不限制，每個等級可重複做多次不同日轉折訊號
                if one_trade_per_level and taken_for_this_level:
                    continue

            trades_df = pd.DataFrame(trades).sort_values(['entry_date', 'exit_date']).reset_index(drop=True)
            if not trades_df.empty:
                trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days
                if show_summary:
                    n  = len(trades_df)
                    wr = (trades_df['pnl'] > 0).mean()
                    tot = (1 + trades_df['pnl_pct']/100).prod() - 1
                    print(f"[{sym}] Trades: {n}, WinRate: {wr:.2%}, TotalRet: {tot:.2%}  "
                        f"(long={sum(trades_df['direction']=='long')}, short={sum(trades_df['direction']=='short')})")

            return trades_df, levels_df

    def backtest_candle_turn_strategy_v66(
            weekly_csv,
            daily_csv,
            *,
            symbol=None,
            tp_pct: float = 0.03,         # 停利 %（當 exit_mode='tp_pct' 時生效）
            retest_tol: float = 0.0,      # 觸碰容忍 (比例)，例 0.001=0.1%
            show_summary: bool = False,
            direct_entry_no_retest: bool = True,
            signal_tf: str = "week",      # 'week' 或 'month'：突破K用的時間框架
            retest_tf: str = "day",       # 'day' 或 'week'：回測K用的時間框架（相容 'daily'/'weekly'）
            monthly_csv = None,           # signal_tf='month' 時可提供月K；未提供則用日線重採樣
            exit_mode: str = "tp_pct",    # ★ 出場模式：'tp_pct' 或 'ma'
            exit_ma_days: int = 20,       # ★ 出場用的日均線長度（exit_mode='ma' 時生效）
            entry_ma_weeks: int = 10,     # ★ 新增：進場濾網用週均線長度（預設10週；當週K≥均線只做多，<均線只做空）
            max_gap_weeks: int | None = None,  # ★ 轉折→首次突破/跌破 相隔超過幾週就忽略（None=不篩）
            export_excel_path: str | None = None,  # ★ 若提供路徑，輸出 trades/levels/summary 到一個 Excel
        ):
        """
        出場條件（擇一）：
        - exit_mode='tp_pct' ：價格相對進場價達到 tp_pct 就出場（多頭：>=；空頭：<=）
        - exit_mode='ma'     ：日線對 SMA(exit_ma_days) 發生反向交叉（多頭：跌破；空頭：突破）
                                * 使用「交叉」避免噪音：prev 在均線同側、當日收盤跨到另一側
        其他邏輯：
        - 以 signal_tf（週/月）偵測『顏色翻轉』求轉折價 L（沿用你的 max/min 寫法）
        - 找到首次「收盤突破/跌破 L」的訊號期 → 僅在「下一個訊號期」內用 retest_tf（日/週）找觸發
        - direct_entry_no_retest=True：不回測，於下一期第一根（retest_tf 粒度）收盤進場

        ★ 新增功能：
        - 進場方向濾網：以「週K相對 N 週均線」決定可做方向（當週K≥均線只做多；<均線只做空）
        - 記錄 gap_weeks：轉折點到首次突破/跌破之間相隔幾週
        - 以 max_gap_weeks 篩選：若 gap_weeks > max_gap_weeks，忽略該轉折（不產生交易）
        - 匯出 Excel（trades/levels/summary），並在 summary/終端輸出 gap_weeks 統計
        """
        import pandas as pd
        import numpy as np
        from pathlib import Path

        # ---------- 讀入日線（並預先算出 exit 用的 SMA） ----------
        dk = pd.read_csv(daily_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        dk[f'EXIT_SMA{exit_ma_days}']      = dk['Close'].rolling(exit_ma_days, min_periods=exit_ma_days).mean()
        dk['prev_Close']                   = dk['Close'].shift(1)
        dk[f'prev_EXIT_SMA{exit_ma_days}'] = dk[f'EXIT_SMA{exit_ma_days}'].shift(1)

        # ---------- 讀入週/月線作為「訊號週期」 ----------
        signal_tf = (signal_tf or "week").lower()
        if signal_tf not in ("week", "month"):
            raise ValueError("signal_tf 必須為 'week' 或 'month'")

        if signal_tf == "week":
            sig = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        else:
            if monthly_csv:
                sig = pd.read_csv(monthly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            else:
                _d = dk.set_index('Date')
                sig = _d.resample('M', label='right', closed='right').agg({
                    'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
                }).dropna(subset=['Open','High','Low','Close']).reset_index()

        # ---------- 由日線重採樣成周線（供 retest_tf='week' 或 進場濾網用） ----------
        wk_from_d = dk.set_index('Date').resample('W-FRI', label='right', closed='right').agg({
            'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
        }).dropna(subset=['Open','High','Low','Close']).reset_index()

        # ---------- 轉折偵測（顏色翻轉 → L） ----------
        def _find_candle_turns(df: pd.DataFrame) -> pd.DataFrame:
            turns = []
            for i in range(1, len(df)):
                prev = df.loc[i-1]
                curr = df.loc[i]
                prev_up   = prev['Close'] > prev['Open']
                prev_down = prev['Close'] < prev['Open']
                curr_up   = curr['Close'] > curr['Open']
                curr_down = curr['Close'] < curr['Open']

                if prev_up and curr_down:
                    L = float(max(prev['Close'], curr['Open']))
                    turns.append({'date': curr['Date'], 'type': 'high', 'price': L})
                if prev_down and curr_up:
                    L = float(min(prev['Close'], curr['Open']))
                    turns.append({'date': curr['Date'], 'type': 'low',  'price': L})
            return pd.DataFrame(turns, columns=['date','type','price']) if len(turns) else pd.DataFrame(columns=['date','type','price'])

        turns = _find_candle_turns(sig)

        # ---------- 失效表（以 signal_tf 收盤判定） ----------
        def _build_levels_table(px: pd.DataFrame, turns_df: pd.DataFrame) -> pd.DataFrame:
            rows = []
            idx_map = {px.loc[i, 'Date']: i for i in range(len(px))}
            for _, t in turns_df.iterrows():
                t_date = pd.to_datetime(t['date'])
                level_type = 'resistance' if t['type']=='high' else 'support'
                L = float(t['price'])
                i0 = idx_map.get(t_date, None)
                invalid_week = pd.NaT
                invalid_reason = ""
                if i0 is not None:
                    for j in range(i0+1, len(px)):
                        c = float(px.loc[j, 'Close'])
                        d = px.loc[j, 'Date']
                        if level_type == 'support':
                            if c < L:
                                invalid_week = d; invalid_reason='close_below_support'; break
                        else:
                            if c > L:
                                invalid_week = d; invalid_reason='close_above_resistance'; break
                rows.append({
                    'turn_week_date': t_date,        # 名稱沿用（即使 signal_tf=month）
                    'level_type': level_type,
                    'level_price': L,
                    'invalid_week': invalid_week,
                    'invalid_reason': invalid_reason,
                    'still_valid': pd.isna(invalid_week)
                })
            return pd.DataFrame(rows).sort_values('turn_week_date').reset_index(drop=True)

        levels_df = _build_levels_table(sig, turns)

        # ---------- 推斷 symbol ----------
        def _infer_symbol(sym, wk_path, dk_path):
            if sym: return str(sym)
            for p in [wk_path, dk_path]:
                try:
                    name = Path(p).stem
                    if name: return name
                except:
                    pass
            return ""
        sym = _infer_symbol(symbol, weekly_csv, daily_csv)

        # ---------- 「下一個訊號期」邊界 + 日/週窗 ----------
        def _next_signal_bounds(i):
            if i + 1 >= len(sig): return None, None
            return sig.loc[i, 'Date'], sig.loc[i+1, 'Date']

        def _days_in_next_period(i):
            b = _next_signal_bounds(i)
            if b == (None, None): return None
            start, end = b
            return dk[(dk['Date'] > start) & (dk['Date'] <= end)].copy()

        def _weeks_in_next_period(i):
            b = _next_signal_bounds(i)
            if b == (None, None): return None
            start, end = b
            return wk_from_d[(wk_from_d['Date'] > start) & (wk_from_d['Date'] <= end)].copy()

        # ---------- 正規化 retest_tf ----------
        _rt = (retest_tf or "day").strip().lower()
        if _rt in ("day", "daily", "d"):
            norm_retest_tf = "day"
        elif _rt in ("week", "weekly", "w"):
            norm_retest_tf = "week"
        else:
            raise ValueError("retest_tf 必須為 'day' 或 'week'（也相容 'daily'/'weekly'）")

        # ---------- 出場邏輯（兩種擇一） ----------
        exit_mode = (exit_mode or "tp_pct").strip().lower()
        if exit_mode not in ("tp_pct", "ma"):
            raise ValueError("exit_mode 必須是 'tp_pct' 或 'ma'")

        def _exit_by_tp(entry_date: pd.Timestamp, side: str, entry_price: float, sl_level: float):
            future = dk[dk['Date'] > entry_date].copy()
            if future.empty: return None
            target = entry_price * (1.0 + float(tp_pct)) if side == 'long' else entry_price * (1.0 - float(tp_pct))

            for _, r in future.iterrows():
                c = float(r['Close'])
                # 先停損、後停利 —— 與 V3 同序
                if side == 'long':
                    if c < sl_level:
                        return (r['Date'], c, 'SL_turn_level_break')
                    if c >= target:
                        return (r['Date'], c, 'TP_pct')
                else:
                    if c > sl_level:
                        return (r['Date'], c, 'SL_turn_level_break')
                    if c <= target:
                        return (r['Date'], c, 'TP_pct')
            r = future.iloc[-1]
            return (r['Date'], float(r['Close']), 'FORCED_LAST')

        def _exit_by_ma(entry_date: pd.Timestamp, side: str, sl_level: float = None):
            col_s  = f'EXIT_SMA{exit_ma_days}'
            col_ps = f'prev_EXIT_SMA{exit_ma_days}'
            future = dk[dk['Date'] > entry_date].copy()
            if future.empty: return None

            for _, r in future.iterrows():
                c  = float(r['Close'])
                pc = float(r['prev_Close']) if not np.isnan(r['prev_Close']) else None
                s  = float(r[col_s])  if not np.isnan(r[col_s])  else None
                ps = float(r[col_ps]) if not np.isnan(r[col_ps]) else None
                if sl_level is not None:
                    if (side == 'long' and c < sl_level) or (side == 'short' and c > sl_level):
                        return (r['Date'], c, 'SL_turn_level_break')
                if s is None or ps is None or pc is None:
                    continue
                if side == 'long':
                    if (pc >= ps) and (c < s):
                        return (r['Date'], c, f'MA{exit_ma_days}_cross')
                else:
                    if (pc <= ps) and (c > s):
                        return (r['Date'], c, f'MA{exit_ma_days}_cross')
            r = future.iloc[-1]
            return (r['Date'], float(r['Close']), 'FORCED_LAST')

        # === 週K均線（進場濾網用） ===
        if entry_ma_weeks is None or int(entry_ma_weeks) <= 0:
            raise ValueError("entry_ma_weeks 必須是正整數")
        if signal_tf == "week":
            sig['ENTRY_SMAw'] = sig['Close'].rolling(int(entry_ma_weeks), min_periods=int(entry_ma_weeks)).mean()
        else:
            wk_from_d['ENTRY_SMAw'] = wk_from_d['Close'].rolling(int(entry_ma_weeks), min_periods=int(entry_ma_weeks)).mean()

        # ---------- 主流程 ----------
        trades = []
        tol_up = 1.0 + float(retest_tol)
        tol_dn = 1.0 - float(retest_tol)

        for _, t in turns.iterrows():
            turn_period_end = pd.to_datetime(t['date'])
            turn_type = t['type']          # 'high' / 'low'
            L = float(t['price'])

            sig_row = sig[sig['Date'] == turn_period_end]
            if sig_row.empty:
                continue
            idx = sig_row.index[0]
            if idx + 1 >= len(sig):
                continue

            future_sig = sig.iloc[idx+1:].copy()
            if turn_type == 'high':
                hit_sig = future_sig[future_sig['Close'] > L]; side='long'
            else:
                hit_sig = future_sig[future_sig['Close'] < L]; side='short'
            if hit_sig.empty:
                continue

            breakout_period_end = pd.to_datetime(hit_sig.iloc[0]['Date'])
            breakout_idx = hit_sig.index[0]

            # gap_weeks
            if signal_tf == 'week':
                gap_weeks = int(breakout_idx - idx)  # 期數差 = 週數
            else:
                gap_weeks = int((breakout_period_end - turn_period_end).days // 7)

            if (max_gap_weeks is not None) and (gap_weeks > int(max_gap_weeks)):
                continue

            # --- 週K均線進場方向濾網（當週K 相對 N 週均線） ---
            # 規則：
            #   - 週收盤 >= N週均線 → 只允許多單，不做空
            #   - 週收盤 <  N週均線 → 只允許空單，不做多
            #   - 若均線不足（NaN），跳過該訊號
            if signal_tf == 'week':
                wrow = sig.loc[breakout_idx]
                w_close = float(wrow['Close'])
                w_sma   = wrow.get('ENTRY_SMAw', np.nan)
            else:
                _w = wk_from_d[wk_from_d['Date'] <= breakout_period_end]
                if _w.empty:
                    continue
                wrow = _w.iloc[-1]
                w_close = float(wrow['Close'])
                w_sma   = wrow.get('ENTRY_SMAw', np.nan)

            if np.isnan(w_sma):
                continue  # 均線尚未形成（資料不足）

            # ★ 關鍵方向限制（「以上只做多；以下只做空」）
            #   * 這裡把「等於」視為「以上」（即允許多單）
            if (w_close >= w_sma and side == 'short') or (w_close < w_sma and side == 'long'):
                continue

            # 只在「下一個訊號期」以日/週找觸發
            window = (_days_in_next_period(hit_sig.index[0]) if norm_retest_tf=='day'
                    else _weeks_in_next_period(hit_sig.index[0]))
            if window is None or window.empty:
                continue

            if direct_entry_no_retest:
                d0 = window.iloc[0]
                trigger_date = d0['Date']
                retest_mode  = 'NO_RETEST_DIRECT'
                entry_date   = trigger_date
                entry_price  = float(d0['Close'])
            else:
                entry_date = None
                entry_price = None
                trigger_date = None
                if norm_retest_tf == 'day':
                    if side == 'long':
                        condA = (window['Low']  <= L * tol_up) & (window['Close'] > window['Open']) & (window['Close'] > L)
                    else:
                        condA = (window['High'] >= L * tol_dn) & (window['Close'] < window['Open']) & (window['Close'] < L)
                    if condA.any():
                        r = window[condA].iloc[0]
                        trigger_date = r['Date']; entry_date = r['Date']; entry_price = float(r['Close'])
                        retest_mode = 'A_touch_bounce'
                    else:
                        found = False
                        for i2 in range(len(window)-1):
                            d1 = window.iloc[i2]; d2 = window.iloc[i2+1]
                            if side == 'long':
                                if (d1['Open'] > d1['Close'] and d1['Close'] < L) and (d2['Open'] < d2['Close'] and d2['Close'] > L):
                                    trigger_date = d2['Date']; entry_date = d2['Date']; entry_price = float(d2['Close'])
                                    retest_mode = 'B_two_day'; found = True; break
                            else:
                                if (d1['Open'] < d1['Close'] and d1['Close'] > L) and (d2['Open'] > d2['Close'] and d2['Close'] < L):
                                    trigger_date = d2['Date']; entry_date = d2['Date']; entry_price = float(d2['Close'])
                                    retest_mode = 'B_two_day'; found = True; break
                        if not found:
                            continue
                else:
                    if side == 'long':
                        condW = (window['Low']  <= L * tol_up) & (window['Close'] > window['Open']) & (window['Close'] > L)
                    else:
                        condW = (window['High'] >= L * tol_dn) & (window['Close'] < window['Open']) & (window['Close'] < L)
                    ok = window[condW]
                    if ok.empty:
                        continue
                    r = ok.iloc[0]
                    trigger_date = r['Date']; entry_date = r['Date']; entry_price = float(r['Close'])
                    retest_mode = 'W_touch_bounce'

            if exit_mode == 'tp_pct':
                exit_pack = _exit_by_tp(entry_date, side, entry_price, L)
            else:
                exit_pack = _exit_by_ma(entry_date, side, sl_level=L)
            if exit_pack is None:
                continue
            exit_date, exit_price, exit_reason = exit_pack

            pnl     = (exit_price - entry_price) if side == 'long' else (entry_price - exit_price)
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'symbol': sym,
                'turn_week_date': turn_period_end,
                'turn_type': t['type'],
                'turn_price': L,
                'breakout_week': breakout_period_end,
                'gap_weeks_from_turn_to_breakout': int(gap_weeks),  # ★ 新增欄位
                'retest_tf': norm_retest_tf,
                'retest_mode': 'NO_RETEST_DIRECT' if direct_entry_no_retest else retest_mode,
                'trigger_date': trigger_date,
                'direction': side,
                'entry_date': entry_date,
                'entry_price': float(entry_price),
                'exit_mode': exit_mode,
                'exit_ma_days': (exit_ma_days if exit_mode=='ma' else None),
                'tp_pct': (float(tp_pct) if exit_mode=='tp_pct' else None),
                'exit_date': exit_date,
                'exit_price': float(exit_price),
                'exit_reason': exit_reason,
                'pnl': float(pnl),
                'pnl_pct': float(pnl_pct),
                'entry_ma_weeks': int(entry_ma_weeks),  # ★ 記錄使用的進場濾網週期
                'filter_week_close': float(w_close),    # 方便追溯
                'filter_week_sma': float(w_sma),        # 方便追溯
            })

        # ---------- 收尾 ----------
        trades_df = pd.DataFrame(trades)
        if trades_df.empty:
            if show_summary:
                print(f"[{sym}] No trades generated. (levels: {len(levels_df)})")
            return trades_df, levels_df

        trades_df = trades_df.sort_values(['entry_date','exit_date']).reset_index(drop=True)
        trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days

        # === gap_weeks 的統計摘要 ===
        gap_col = 'gap_weeks_from_turn_to_breakout'
        gap_stats = {}
        try:
            g = trades_df[gap_col].dropna().astype(int)
            if len(g) > 0:
                gap_stats = {
                    'count': int(g.count()),
                    'min': int(g.min()),
                    'max': int(g.max()),
                    'mean': float(g.mean()),
                    'median': float(g.median()),
                }
            else:
                gap_stats = {'count': 0, 'min': None, 'max': None, 'mean': None, 'median': None}
        except Exception:
            gap_stats = {'count': 0, 'min': None, 'max': None, 'mean': None, 'median': None}

        # 分佈區間（可依需求調整）
        def _bucket(x: int):
            if x <= 2: return '0-2'
            if x <= 4: return '3-4'
            if x <= 8: return '5-8'
            return '9+'
        try:
            dist = trades_df[gap_col].dropna().astype(int).map(_bucket).value_counts().to_dict()
        except Exception:
            dist = {}

        # 總績效
        n_trades = len(trades_df)
        win_rate = (trades_df['pnl'] > 0).mean()
        total_ret = (1 + trades_df['pnl_pct']/100).prod() - 1
        long_n = int((trades_df['direction']=='long').sum())
        short_n = n_trades - long_n

        # 依勝敗的 gap 平均
        try:
            gap_by_win = trades_df.assign(win=(trades_df['pnl']>0)).groupby('win')[gap_col].mean().to_dict()
        except Exception:
            gap_by_win = {}

        if show_summary:
            mean_str = f"{gap_stats['mean']:.2f}" if gap_stats.get('mean') is not None else "None"
            print(
                f"[{sym}] Trades: {n_trades}, WinRate: {win_rate:.2%}, TotalRet: {total_ret:.2%}  "
                f"(long={long_n}, short={short_n})"
            )
            print(
                f"gap_weeks: count={gap_stats.get('count')}, min={gap_stats.get('min')}, "
                f"median={gap_stats.get('median')}, mean={mean_str}, max={gap_stats.get('max')}"
            )
            if dist:
                print(f"gap buckets: {dist}")
            if gap_by_win:
                print(f"avg gap by win/loss: {gap_by_win}")

        # === 輸出 Excel（trades/levels/summary 三工作表） ===
        if export_excel_path:
            summary_rows = [
                {'metric': 'symbol', 'value': sym},
                {'metric': 'trades', 'value': n_trades},
                {'metric': 'win_rate', 'value': f"{win_rate:.4f}"},
                {'metric': 'total_ret', 'value': f"{total_ret:.6f}"},
                {'metric': 'long_trades', 'value': long_n},
                {'metric': 'short_trades', 'value': short_n},
                {'metric': 'max_gap_weeks_filter', 'value': max_gap_weeks},
                {'metric': 'gap_count', 'value': gap_stats.get('count')},
                {'metric': 'gap_min', 'value': gap_stats.get('min')},
                {'metric': 'gap_median', 'value': gap_stats.get('median')},
                {'metric': 'gap_mean', 'value': gap_stats.get('mean')},
                {'metric': 'gap_max', 'value': gap_stats.get('max')},
                {'metric': 'entry_ma_weeks', 'value': int(entry_ma_weeks)},
            ]
            for k, v in dist.items():
                summary_rows.append({'metric': f'gap_bucket_{k}', 'value': v})
            for k, v in gap_by_win.items():
                label = 'win_true' if k is True else 'win_false'
                summary_rows.append({'metric': f'avg_gap_{label}', 'value': v})

            summary_df = pd.DataFrame(summary_rows, columns=['metric','value'])

            with pd.ExcelWriter(export_excel_path) as xw:
                trades_df.to_excel(xw, index=False, sheet_name='trades')
                levels_df.to_excel(xw, index=False, sheet_name='levels')
                summary_df.to_excel(xw, index=False, sheet_name='summary')

        return trades_df, levels_df

    def backtest_candle_turn_strategy_v7(
            weekly_csv,
            daily_csv,
            *,
            symbol=None,
            tp_pct: float = 0.03,
            show_summary: bool = False,
            signal_tf: str = "week",
            monthly_csv = None,
            exit_mode: str = "tp_pct",
            exit_ma_days: int = 20,
            max_gap_weeks: int | None = None,
            export_excel_path: str | None = None,
        ):
        dk = pd.read_csv(daily_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        dk[f'EXIT_SMA{exit_ma_days}']      = dk['Close'].rolling(exit_ma_days, min_periods=exit_ma_days).mean()
        dk['prev_Close']                   = dk['Close'].shift(1)
        dk[f'prev_EXIT_SMA{exit_ma_days}'] = dk[f'EXIT_SMA{exit_ma_days}'].shift(1)

        signal_tf_l = (signal_tf or "week").lower()
        if signal_tf_l not in ("week", "month"):
            raise ValueError("signal_tf 必須為 'week' 或 'month'")

        if signal_tf_l == "week":
            sig = pd.read_csv(weekly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
        else:
            if monthly_csv:
                sig = pd.read_csv(monthly_csv, parse_dates=['Date']).sort_values('Date').reset_index(drop=True)
            else:
                _d = dk.set_index('Date')
                sig = _d.resample('M', label='right', closed='right').agg({
                    'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'
                }).dropna(subset=['Open','High','Low','Close']).reset_index()

        def _find_candle_turns(df: pd.DataFrame) -> pd.DataFrame:
            turns = []
            for i in range(1, len(df)):
                prev = df.loc[i-1]
                curr = df.loc[i]
                prev_up   = prev['Close'] > prev['Open']
                prev_down = prev['Close'] < prev['Open']
                curr_up   = curr['Close'] > curr['Open']
                curr_down = curr['Close'] < curr['Open']
                if prev_up and curr_down:
                    L = float(max(prev['Close'], curr['Open']))
                    turns.append({'date': curr['Date'], 'type': 'high', 'price': L})
                if prev_down and curr_up:
                    L = float(min(prev['Close'], curr['Open']))
                    turns.append({'date': curr['Date'], 'type': 'low',  'price': L})
            return pd.DataFrame(turns, columns=['date','type','price']) if len(turns) else pd.DataFrame(columns=['date','type','price'])

        turns = _find_candle_turns(sig)

        def _build_levels_table(px: pd.DataFrame, turns_df: pd.DataFrame) -> pd.DataFrame:
            rows = []
            idx_map = {px.loc[i, 'Date']: i for i in range(len(px))}
            for _, t in turns_df.iterrows():
                t_date = pd.to_datetime(t['date'])
                level_type = 'resistance' if t['type']=='high' else 'support'
                L = float(t['price'])
                i0 = idx_map.get(t_date, None)
                invalid_week = pd.NaT
                invalid_reason = ""
                if i0 is not None:
                    for j in range(i0+1, len(px)):
                        c = float(px.loc[j, 'Close'])
                        d = px.loc[j, 'Date']
                        if level_type == 'support':
                            if c < L:
                                invalid_week = d; invalid_reason='close_below_support'; break
                        else:
                            if c > L:
                                invalid_week = d; invalid_reason='close_above_resistance'; break
                rows.append({
                    'turn_week_date': t_date,
                    'level_type': level_type,
                    'level_price': L,
                    'invalid_week': invalid_week,
                    'invalid_reason': invalid_reason,
                    'still_valid': pd.isna(invalid_week)
                })
            return pd.DataFrame(rows).sort_values('turn_week_date').reset_index(drop=True)

        levels_df = _build_levels_table(sig, turns)

        def _infer_symbol(sym, wk_path, dk_path):
            if sym: return str(sym)
            for p in [wk_path, dk_path]:
                try:
                    name = Path(p).stem
                    if name: return name
                except Exception:
                    pass
            return ""
        sym = _infer_symbol(symbol, weekly_csv, daily_csv)

        trades = []
        for _, t in turns.iterrows():
            turn_period_end = pd.to_datetime(t['date'])
            turn_type = t['type']
            L = float(t['price'])

            sig_row = sig[sig['Date'] == turn_period_end]
            if sig_row.empty:
                continue
            idx = sig_row.index[0]
            if idx + 2 > len(sig) - 0:
                continue

            future_sig = sig.iloc[idx+1:].copy()
            if turn_type == 'high':
                hit_sig = future_sig[future_sig['Close'] > L]; initial_break='up'
            else:
                hit_sig = future_sig[future_sig['Close'] < L]; initial_break='down'
            if hit_sig.empty:
                continue

            breakout_idx = hit_sig.index[0]
            breakout_period_end = pd.to_datetime(sig.loc[breakout_idx, 'Date'])

            if signal_tf_l == 'week':
                gap_weeks = int(breakout_idx - idx)
            else:
                gap_weeks = int((breakout_period_end - turn_period_end).days // 7)

            if (max_gap_weeks is not None) and (gap_weeks > int(max_gap_weeks)):
                continue

            next_idx = breakout_idx + 1
            if next_idx >= len(sig):
                continue
            next_row   = sig.loc[next_idx]
            next_close = float(next_row['Close'])
            next_date  = pd.to_datetime(next_row['Date'])

            enter = False
            side  = None
            if initial_break == 'up' and next_close < L:
                enter = True; side = 'short'
            elif initial_break == 'down' and next_close > L:
                enter = True; side = 'long'
            if not enter:
                continue

            entry_date  = next_date
            entry_price = next_close
            trigger_date = next_date
            retest_mode  = 'NEXT_PERIOD_OPPOSITE_WEEKLY_CLOSE'

            # exits using daily series
            def _exit_by_tp(entry_date: pd.Timestamp, side: str, entry_price: float, sl_level: float):
                future = dk[dk['Date'] > entry_date].copy()
                if future.empty: return None
                target = entry_price * (1.0 + float(tp_pct)) if side == 'long' else entry_price * (1.0 - float(tp_pct))
                for _, r in future.iterrows():
                    c = float(r['Close'])
                    if side == 'long':
                        if c < sl_level:
                            return (r['Date'], c, 'SL_turn_level_break')
                        if c >= target:
                            return (r['Date'], c, 'TP_pct')
                    else:
                        if c > sl_level:
                            return (r['Date'], c, 'SL_turn_level_break')
                        if c <= target:
                            return (r['Date'], c, 'TP_pct')
                r = future.iloc[-1]
                return (r['Date'], float(r['Close']), 'FORCED_LAST')

            def _exit_by_ma(entry_date: pd.Timestamp, side: str, sl_level: float = None):
                col_s  = f'EXIT_SMA{exit_ma_days}'
                col_ps = f'prev_EXIT_SMA{exit_ma_days}'
                future = dk[dk['Date'] > entry_date].copy()
                if future.empty: return None
                for _, r in future.iterrows():
                    c  = float(r['Close'])
                    pc = float(r['prev_Close']) if not np.isnan(r['prev_Close']) else None
                    s  = float(r[col_s])  if not np.isnan(r[col_s])  else None
                    ps = float(r[col_ps]) if not np.isnan(r[col_ps]) else None
                    if sl_level is not None:
                        if (side == 'long' and c < sl_level) or (side == 'short' and c > sl_level):
                            return (r['Date'], c, 'SL_turn_level_break')
                    if s is None or ps is None or pc is None:
                        continue
                    if side == 'long':
                        if (pc >= ps) and (c < s):
                            return (r['Date'], c, f'MA{exit_ma_days}_cross')
                    else:
                        if (pc <= ps) and (c > s):
                            return (r['Date'], c, f'MA{exit_ma_days}_cross')
                r = future.iloc[-1]
                return (r['Date'], float(r['Close']), 'FORCED_LAST')

            if exit_mode == 'tp_pct':
                exit_pack = _exit_by_tp(entry_date, side, entry_price, L)
            else:
                exit_pack = _exit_by_ma(entry_date, side, sl_level=L)
            if exit_pack is None:
                continue
            exit_date, exit_price, exit_reason = exit_pack

            pnl     = (exit_price - entry_price) if side == 'long' else (entry_price - exit_price)
            pnl_pct = (pnl / entry_price) * 100.0

            trades.append({
                'symbol': sym,
                'turn_week_date': turn_period_end,
                'turn_type': turn_type,
                'turn_price': L,
                'breakout_week': pd.to_datetime(breakout_period_end),
                'gap_weeks_from_turn_to_breakout': int(gap_weeks),
                'retest_tf': 'signal_tf_only',
                'retest_mode': retest_mode,
                'trigger_date': trigger_date,
                'direction': side,
                'entry_date': entry_date,
                'entry_price': float(entry_price),
                'exit_mode': exit_mode,
                'exit_ma_days': (exit_ma_days if exit_mode=='ma' else None),
                'tp_pct': (float(tp_pct) if exit_mode=='tp_pct' else None),
                'exit_date': exit_date,
                'exit_price': float(exit_price),
                'exit_reason': exit_reason,
                'pnl': float(pnl),
                'pnl_pct': float(pnl_pct),
            })

        trades_df = pd.DataFrame(trades)
        if trades_df.empty:
            if show_summary:
                print(f"[{sym}] No trades generated. (levels: {len(levels_df)})")
            return trades_df, levels_df

        trades_df = trades_df.sort_values(['entry_date','exit_date']).reset_index(drop=True)
        trades_df['holding_days'] = (trades_df['exit_date'] - trades_df['entry_date']).dt.days

        gap_col = 'gap_weeks_from_turn_to_breakout'
        gap_stats = {}
        try:
            g = trades_df[gap_col].dropna().astype(int)
            if len(g) > 0:
                gap_stats = {
                    'count': int(g.count()),
                    'min': int(g.min()),
                    'max': int(g.max()),
                    'mean': float(g.mean()),
                    'median': float(g.median()),
                }
            else:
                gap_stats = {'count': 0, 'min': None, 'max': None, 'mean': None, 'median': None}
        except Exception:
            gap_stats = {'count': 0, 'min': None, 'max': None, 'mean': None, 'median': None}

        def _bucket(x: int):
            if x <= 2: return '0-2'
            if x <= 4: return '3-4'
            if x <= 8: return '5-8'
            return '9+'
        try:
            dist = trades_df[gap_col].dropna().astype(int).map(_bucket).value_counts().to_dict()
        except Exception:
            dist = {}

        n_trades = len(trades_df)
        win_rate = (trades_df['pnl'] > 0).mean()
        total_ret = (1 + trades_df['pnl_pct']/100).prod() - 1
        long_n = int((trades_df['direction']=='long').sum())
        short_n = n_trades - long_n

        try:
            gap_by_win = trades_df.assign(win=(trades_df['pnl']>0)).groupby('win')[gap_col].mean().to_dict()
        except Exception:
            gap_by_win = {}

        if show_summary:
            print(
                f"[{sym}] Trades: {n_trades}, WinRate: {win_rate:.2%}, TotalRet: {total_ret:.2%}  "
                f"(long={long_n}, short={short_n})"
            )
            mean_str = f"{gap_stats['mean']:.2f}" if gap_stats.get('mean') is not None else "None"
            print(f"gap_weeks: count={gap_stats.get('count')}, min={gap_stats.get('min')}, "
                f"median={gap_stats.get('median')}, mean={mean_str}, max={gap_stats.get('max')}")
            if dist:
                print(f"gap buckets: {dist}")
            if gap_by_win:
                print(f"avg gap by win/loss: {gap_by_win}")

        if export_excel_path:
            summary_rows = [
                {'metric': 'symbol', 'value': sym},
                {'metric': 'trades', 'value': n_trades},
                {'metric': 'win_rate', 'value': f"{win_rate:.4f}"},
                {'metric': 'total_ret', 'value': f"{total_ret:.6f}"},
                {'metric': 'long_trades', 'value': long_n},
                {'metric': 'short_trades', 'value': short_n},
                {'metric': 'max_gap_weeks_filter', 'value': max_gap_weeks},
                {'metric': 'gap_count', 'value': gap_stats.get('count')},
                {'metric': 'gap_min', 'value': gap_stats.get('min')},
                {'metric': 'gap_median', 'value': gap_stats.get('median')},
                {'metric': 'gap_mean', 'value': gap_stats.get('mean')},
                {'metric': 'gap_max', 'value': gap_stats.get('max')},
            ]
            for k, v in dist.items():
                summary_rows.append({'metric': f'gap_bucket_{k}', 'value': v})
            for k, v in gap_by_win.items():
                label = 'win_true' if k is True else 'win_false'
                summary_rows.append({'metric': f'avg_gap_{label}', 'value': v})

            summary_df = pd.DataFrame(summary_rows, columns=['metric','value'])

            with pd.ExcelWriter(export_excel_path) as xw:
                trades_df.to_excel(xw, index=False, sheet_name='trades')
                levels_df.to_excel(xw, index=False, sheet_name='levels')
                summary_df.to_excel(xw, index=False, sheet_name='summary')

        return trades_df, levels_df

    

G_data=False
G_D2W=False
G_Test=False
G_draw=False
G_sma_WeekDay=True
G_VA_WeekDay=False
data=collectdata(txt="Oil.txt", strategy="VA", d_entry=True)
if G_data==True:
    data.Get_data()
if G_D2W==True:
    data.D2W()
    data.D2M()
if G_Test==True:
    #data.VVWM("DK")
    data.SMA("WK")
if G_sma_WeekDay==True:
    #exit_mode="ma", ma_num exit_ma_days=20
    #exit_mode="tp_pct", tp_pct=0.02,
    data.A_period="week"
    data.B_period="day"
    data.exit_mode="tp_pct"
    data.ma_num=5
    data.percent=0.02
    data.gap_week=20
    data.batch_backtest_sma_strategy()
'''
# 設定股票代號
symbol = '2330.TW'  # 以台積電(2330)為例
# 使用yfinance套件抓取台股資料
stock = yf.Ticker(symbol)
# 設定開始和結束日期
start_date = '2018-01-01'
end_date = '2020-12-31'
# 抓取台股歷史價格資料
history = stock.history(start=start_date, end=end_date)
history['Date'] = history.index.strftime('%Y-%m-%d')
# 印出結果
history.to_csv('stock_data.csv')
'''
