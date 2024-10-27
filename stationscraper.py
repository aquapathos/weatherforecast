import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import re

class AmedasStationScraper(object):
    """
    stations = AmedasStationScraper().stations で stations に取得した観測所情報を格納
    """
    def __init__(self, file_path = None, encoding='utf-8'):
        self.encoding = encoding

        if file_path is None: # csvファイルが与えられていない場合は掻き集めてくる
            self.stations = self.scrap()
        else:
            self.stations = pd.read_csv( file_path, encoding = self.encoding)

    # 指定されたURL用のBeautifulSoupを生成
    def get_soup(self, url):
        html = requests.get(url)
        html.encoding = self.encoding
        soup = BeautifulSoup(html.text, "html.parser")
        return soup

    # 気象庁データから気象データを取得
    def scrap(self):
        area_list, area_link_list = self.get_all_area_link()
        return self.get_all_stations(area_list, area_link_list)
        
    # エリアごとのページへのリンクを取り出す
    def get_all_area_link(self): 
        url = 'https://www.data.jma.go.jp/obd/stats/etrn/select/prefecture00.php?prec_no=&block_no=&year=&month=&day=&view='
        soup = self.get_soup(url)
        elements = soup.find_all('area')  # area（地域名）を取り出す
        area_list = [element['alt'] for element in elements] # alt（観測所名）を取り出す
        area_link_list = [element['href'] for element in elements] # URLを取り出す
        return area_list, area_link_list

    # 与えられたエリアページを訪問し、全観測局のリンクを取り出してデータフレーム化
    def get_all_stations(self, area_list, area_link_list) -> pd.DataFrame:
        dflists = []
        for area, area_link in tqdm(zip(area_list, area_link_list)):
            dflists.append(self.get_station_data(area, area_link))
        stdf = pd.concat(dflists).reset_index(drop=True)
        return self.format_df(stdf)

    # 与えられたエリアページを訪問し、全観測局のリンクを取り出してデータフレーム化
    def get_station_data(self, area, area_link) -> pd.DataFrame:
        url = 'https://www.data.jma.go.jp/obd/stats/etrn/select/' + area_link
        soup = self.get_soup(url)
        areas = soup.find_all('area')  # area（地域名）をすべて取り出す
        station_list = [area['alt'] for area in areas] # alt（観測所名）を取り出す
        station_link_list = [area['href'].strip( # href=../以降を取り出す
            '../') for area in areas]
        station_info = [area['onmouseover'] if area.has_attr(
            'onmouseover') else '-' for area in areas]
        assert len(station_list) == len(station_link_list)
        data = {'観測所': station_list,
                'url': station_link_list, 'info': station_info}
                
        df = pd.DataFrame(data)
        df['地域'] = area
        return df[['地域', '観測所', 'url', 'info']]

    def defaultfind(self, pattern, s, default=None, callback=None):
        cont = re.findall(pattern, s)
        if len(cont) == 0:
            return default
        else:
            if callback is not None:
                return callback(cont[0])
            return cont[0]

    def format_df(self, df):
        # prec_no
        df['prec_no'] = df.url.apply(
            lambda x: self.defaultfind("prec_no=\d{1,2}&", x))
        df = df.dropna()
        df.prec_no = df.prec_no.apply(lambda x: x[8:-1])
        # block_no
        df['block_no'] = df.url.apply(
            lambda x: self.defaultfind("block_no=\d{4,6}&", x))
        df = df.dropna()
        df.block_no = df.block_no.apply(lambda x: x[9:-1])
        # type
        df['type'] = df['info'].apply(lambda x: self.defaultfind("Point\('.'", x))
        df = df.dropna()
        df['type'] = df['type'].apply(lambda x: x[7:-1])
        # 基地局名カナ読み
        df['観測所カナ'] = df['info'].apply(
            lambda x: self.defaultfind(r'Point.*?(?:,.*?){2},\s*(.*?)\s*,', x))
        df = df.dropna()
        df['観測所カナ'] = df['観測所カナ'].apply(lambda x: x[1:-1])
        # 緯度
        df['lat0'] = df['info'].apply(
            lambda x: self.defaultfind(r'Point.*?(?:,.*?){3},\s*(.*?)\s*,', x))
        df = df.dropna()
        df['lat0'] = df['lat0'].apply(lambda x: x[1:-1])
        df['lat1'] = df['info'].apply(
            lambda x: self.defaultfind(r'Point.*?(?:,.*?){4},\s*(.*?)\s*,', x))
        df = df.dropna()
        df['lat1'] = df['lat1'].apply(lambda x: x[1:-1])
        df['緯度'] = df['lat0'].astype(float) + df['lat1'].astype(float)/60
        # 軽度
        df['lon0'] = df['info'].apply(
            lambda x: self.defaultfind(r'Point.*?(?:,.*?){5},\s*(.*?)\s*,', x))
        df = df.dropna()
        df['lon0'] = df['lon0'].apply(lambda x: x[1:-1])
        df['lon1'] = df['info'].apply(
            lambda x: self.defaultfind(r'Point.*?(?:,.*?){6},\s*(.*?)\s*,', x))
        df = df.dropna()
        df['lon1'] = df['lon1'].apply(lambda x: x[1:-1])
        df['経度'] = df['lon0'].astype(float) + df['lon1'].astype(float)/60
        # 標高
        df['alt'] = df['info'].apply(
            lambda x: self.defaultfind(r'Point.*?(?:,.*?){7},\s*(.*?)\s*,', x))
        df = df.dropna()
        df['標高'] = df['alt'].apply(lambda x: float(x[1:-1]))
        # 不要部分削除
        df.drop(['url', 'info', 'lat0', 'lat1', 'lon0', 'lon1','alt'], inplace=True, axis=1)
        df.drop_duplicates(inplace=True)
        return df.reset_index(drop=True)