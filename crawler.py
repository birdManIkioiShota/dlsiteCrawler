import sys
from bs4 import BeautifulSoup as bs
import requests
import urllib.robotparser
import pandas as pd
import time
from tqdm import tqdm

class Clawler:
    __url = 'https://www.dlsite.com/maniax/fsr/=/language/jp/sex_category%5B0%5D/male/ana_flg/off/order%5B0%5D/release_d/genre_and_or/or/options_and_or/or/options%5B0%5D/JPN/options%5B1%5D/NM/options_name%5B0%5D/%E6%97%A5%E6%9C%AC%E8%AA%9E%E4%BD%9C%E5%93%81/options_name%5B1%5D/%E8%A8%80%E8%AA%9E%E4%B8%8D%E5%95%8F%E4%BD%9C%E5%93%81/file_type_category%5B0%5D/audio_file/file_type_category_name%5B0%5D/%E3%82%AA%E3%83%BC%E3%83%87%E3%82%A3%E3%82%AA%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB/per_page/100/show_type/1/from/fs.detail/page/'
    __robotsTxt = 'https://www.dlsite.com/robots.txt'
    __robo = urllib.robotparser.RobotFileParser()
    __waitTime: int = 1

    def __init__(self):
        print("initializing...", end="")
        self.__robo.set_url(self.__robotsTxt)
        self.__robo.read()

        # robots.txtで禁止されていたら強制終了
        if(not self.__robo.can_fetch('*', self.__url)):
            print("access denided!")
            sys.exit()
        print("done!")

        # crawl_delayを更新
        delay = self.__robo.crawl_delay('*')
        if(delay is not None and delay > self.__waitTime):
            self.__waitTime = delay

    def crawlDLsite(self):
        print('crawling...')

        page = 1
        html = requests.get(self.__url + str(page))
        soup = bs(html.text, 'html.parser')
        
        # 総ページ数の取得
        total = soup.find(class_="page_total").find("strong").text
        max_page = (int(total) // 100) + 1
        bar = tqdm(total = max_page) # プログレスバーの初期化

        data = pd.DataFrame(data=None, index=None, dtype=None, copy=False)

        while(page <= max_page):
            bar.update(1)
            if(soup.find('title').text == '404 | DLsite'): return

            self.__parseHtml(soup).to_csv('./onseiData.csv', mode='a', header=False, index=False)

            time.sleep(self.__waitTime)
            page = page + 1
            html = requests.get(self.__url + str(page))
            soup = bs(html.text, 'html.parser')

    def __parseHtml(self, soup: bs) -> pd.DataFrame:
        """
        返り値のcol={
            'タイトル',
            'サークル名',
            '声優',
            'タグ',
            '定価'
        }

        注意：DLsite側の検索ページ表示上限が100件なので、
        　　　呼び出し側でページ数を更新しながら呼び出す必要があります
        """
        title = []
        circle = []
        cast = []
        tag = []
        date = []
        sales = []
        price = []

        works = soup.find_all("tr")
        for work in works:
            # 作品以外をふるい落とす.
            if(work.find(class_="work_name") == None): continue
            
            # タイトル
            title.append(work.find(class_="work_name").find("a").text)

            # サークル名
            circleName = work.find(class_="maker_name")
            circle.append(circleName.find('a').text.replace('\n', ''))
            
            # 声優名
            castList = ''
            casts = work.find_all(class_="author")
            if(len(casts) != 0):
                for a_cast in casts:
                    castList = castList + '\t' +  a_cast.text
            cast.append(castList[1:])

            # タグ
            tagList = ''
            tags = work.find_all(class_="search_tag")
            if(len(tags) != 0):
                for a_tag in tags:
                    tagList = tagList + a_tag.text.replace('\n', '\t')
            tag.append(tagList[1:])

            # 販売日 & 売上
            aaa = work.find(class_="work_info_box").text.replace('\n', '')

            # 定価
            price_this: str
            if(work.find(class_="strike") is not None): # 値引き中の定価はこっち
                price_this = work.find(class_="strike").text
            else:
                price_this = work.find(class_="work_price").text # 定価販売中の価格
            price_this = price_this.replace('円', '')
            price_this = price_this.replace(',', '')
            price.append(int(price_this))

        df = pd.DataFrame(title, columns={'タイトル'})
        # df = df.join(pd.DataFrame(circle, columns={'サークル名'}))
        # df = df.join(pd.DataFrame(cast, columns={'声優'}))
        df = df.join(pd.DataFrame(tag, columns={'タグ'}))
        # df = df.join(pd.DataFrame(price, columns={'定価'}))
        
        return df

hoge = Clawler()
hoge.crawlDLsite()