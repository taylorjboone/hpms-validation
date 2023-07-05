import requests 
from bs4 import BeautifulSoup 
import os  
import urllib.parse

url1 = 'https://gis.transportation.wv.gov/ftp/TMA/HPMS2019data_20sub/Section_April/'
url2 = 'https://gis.transportation.wv.gov/ftp/TMA/HPMS2019data_20sub/Section_June/'
url3 = 'https://gis.transportation.wv.gov/ftp/TMA/HPMS2019data_20sub/Section_June/Traffic/'
url4 = 'https://gis.transportation.wv.gov/ftp/TMA/HPMS2019data_20sub/Section_June/TTR/'
url5 = 'https://gis.transportation.wv.gov/ftp/TMA/HPMS2019data_20sub/Pavement/'
domain = 'https://gis.transportation.wv.gov'
total_urls = []

for url in [url1,url2,url3,url4,url5]:

    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    vals = soup.find_all('a')

    total_urls += [f'{domain}/{val.get_attribute_list("href")[0]}' for val in vals]
# urls = [f'{domain}/{val.get_attribute_list("href")[0]}' for val in vals]

# r = requests.get(url2)
# soup = BeautifulSoup(r.text)
# vals = soup.find_all('a')

# total_urls += [f'{domain}/{val.get_attribute_list("href")[0]}' for val in vals]
urls = total_urls
urls = [url for url in urls if url.endswith('.csv')]


print(len(urls))
outdir = '2023_errors/2019_erros'
# if not os.path.exists(outdir):
    # os.mkdir(outdir)

for pos,url in enumerate(urls):
    fn = url.split('/')[-1]
    fn = urllib.parse.unquote(fn)
    # print(url)
    r = requests.get(urllib.parse.unquote(url))
    with open(f'{outdir}/{fn}','wb') as f:
        f.write(r.content)
    print(f'{pos}/{len(urls)}')
