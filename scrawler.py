import codecs
import os
import requests
from bs4 import BeautifulSoup
import simpleutil
from multiprocessing.pool import Pool

FOLDER_HTML = 'html'
FILE_CITYLIST = '%s/citylist.html'%FOLDER_HTML

FOLDER_RESULT = 'result'
FILE_CITYLIST_RESULT = '%s/citylist.txt'%FOLDER_RESULT
FILE_WEATHER_RESULT = '%s/weatherdetail.txt'%FOLDER_RESULT

URL_ROOT = 'http://www.tianqihoubao.com'
URL_CITYLIST = '%s/lishi/'%URL_ROOT


def url_history(city, month):
     # 'http://www.tianqihoubao.com/lishi/taiyuan/month/201101.html'
    return 'http://www.tianqihoubao.com/lishi/{0}/month/{1}.html'.format(city, month)


def filepath_history(city, month):
    return '%s/%s_%s.html'%(FOLDER_HTML, city, month)


def filepath_history_result(city, month):
    return '%s/%s_%s.html'%(FOLDER_RESULT, city, month)


def download_city():
    text_root = requests.get(URL_ROOT).text
    simpleutil.write_txt(FILE_CITYLIST, text_root)


def download_weather_all():
    years = ['2011', '2012', '2013', '2014', '2015', '2016', '2017']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    dataset_city = simpleutil.read_dataset(FILE_CITYLIST_RESULT)
    city_codes = [x[4] for x in dataset_city]

    keys = list()
    for city in city_codes:
        for year in years:
            for month in months:
                keys.append(city + '_' + year+month)
    Pool(processes=5).map(download_weather_detail, keys)


def download_weather_detail(city_month):
    city, month = city_month.split('_')
    url_city = url_history(city, month)
    filepath = filepath_history(city, month)
    if os.path.exists(filepath):
        return
    print('download %s' % url_city)
    text_root = requests.get(url_city).text
    simpleutil.write_txt(filepath, text_root)


def analyze_city():
    """
    解析出城市列表
    :return:
    """
    text_root = simpleutil.read_txt(FILE_CITYLIST)
    bs = BeautifulSoup(text_root)
    tag_citylist_root = bs.find_all('div', class_='citychk')[0]

    tbl_detail_city = list()
    for child in tag_citylist_root.find_all('dl'):
        tags_citys = child.find_all('a')
        prov_citys = list()
        for tag_city in tags_citys:
            city_href = tag_city.attrs['href']
            city_name = tag_city.attrs['title'].replace('历史天气预报', '').replace('历史天气查询', '')
            prov_citys.append((city_href, city_name))

        for city in prov_citys[1:]:
            # 省名，省代号，省url，市名，市代号，市url
            tbl_detail_city.append((prov_citys[0][1], prov_citys[0][0].split('/')[-1][:-4], URL_ROOT + prov_citys[0][0],
                                   city[1], city[0].split('/')[-1][:-5], URL_ROOT + city[0]))

    simpleutil.write_dataset(FILE_CITYLIST_RESULT, tbl_detail_city)


def analyze_simple():
    """
    解析出各城市各月份的天气信息
    :return:
    """
    listfile = os.listdir(FOLDER_HTML)

    for idx, filepath in enumerate(listfile):
        print(filepath)
        if idx % 1000 == 0:
            print(idx)
        filepath = FOLDER_HTML + '/' + filepath
        print(filepath, FILE_CITYLIST)
        if filepath == FILE_CITYLIST:
            continue
        citycode, month = filepath.split('/')[-1][:-5].split('_')
        filepath_result = filepath_history_result(citycode, month)
        if os.path.exists(filepath_result):
            continue
        text = simpleutil.read_txt(filepath)
        bs = BeautifulSoup(text)
        tbl = bs.find_all('table')[0]
        dataset = list()
        for idx, tr in enumerate(tbl.find_all('tr')):
            if idx == 0:
                continue
            tds = tr.find_all('td')
            datestr = tds[0].find_all('a')[0].text
            date = datestr.replace('年', '-').replace('月', '-').replace('日', '').strip()
            weather = tds[1].text.replace('\n', '')
            tempreture = tds[2].text.replace('\n', '')
            wind = tds[3].text.replace('\n', '')
            # 市代号，月份，日期，天气，温度，风力
            dataset.append([citycode, month, date, weather, tempreture, wind])
        simpleutil.write_dataset(filepath_result, dataset)


def combine():
    listfile = os.listdir(FOLDER_RESULT)
    fileout = codecs.open(FILE_WEATHER_RESULT, 'w', 'utf-8')
    for idx, filepath in enumerate(listfile):
        if idx % 100 == 0:
            print(idx)
        filepath = FOLDER_RESULT + '/' + filepath
        if filepath == FILE_CITYLIST_RESULT:
            continue
        dataset = simpleutil.read_dataset(filepath)
        for data in dataset:
            linestr = '\t'.join(data)
            if len(linestr) == 0:
                continue
            fileout.write(linestr + '\n')
    fileout.close()


if __name__ == '__main__':
    # download_city()
    # analyze_city()
    # download_weather_all()
    # analyze_simple()
    combine()
