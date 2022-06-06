"""
Парсим аудиокниги с RUTOR.INFO
"""

from bs4 import BeautifulSoup
import requests
import re
from fp.fp import FreeProxy
import sqlite3
import time

sql_make = """
CREATE TABLE IF NOT EXISTS abooks (
    aid      INTEGER PRIMARY KEY,
    fullname TEXT,
    name     TEXT,
    author   TEXT,
    year     TEXT,
    genre    TEXT,
    reader   TEXT,
    est      TEXT,
    desc     TEXT,
    url      TEXT,
    magnet   TEXT
)"""
# sql_add = "INSERT OR IGNORE INTO abooks VALUES(?,?,?,?,?,?,?,?,?,?,?)"
sql_add = "INSERT OR REPLACE INTO abooks VALUES(?,?,?,?,?,?,?,?,?,?,?)"

con = sqlite3.connect('rutor.db')
con.execute(sql_make)

# Декоратор для статических переменных
def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate

# Ищем прокси для доступа к RUTOR
def get_proxy():
    title_re = re.compile(r'<title>(.*?)</title>')
    while True:
        print('[Find proxy]')
        try:
            proxy = FreeProxy(rand=True).get()
            print(f'Proxy found: {proxy}')
            proxy = {proxy.split(':')[0]: proxy}
        except Exception as e:
            print(str(e))
            time.sleep(5)
            continue

        print('[Check access to RUTOR.INFO]')
        try:
            page = requests.get('http://rutor.info', proxies=proxy, timeout=5)
            good_request = page.status_code == 200
        except:
            good_request = False

        if good_request == False:
            print('Bad proxy. Access is denied.\n')
            continue

        # Проверим, что это RUTOR, а не Роскомнадзор
        match = title_re.search(page.text)
        if match and ('rutor' in match.group(1).lower()):
            print('Accessed.')
            return proxy
        else:
            print('Big Brother Roskomnadzor.')

# Получим страницу с сервера
@static_vars(valid_proxy = False)
def get_page(url):
    # get_page.valid_proxy=vars(get_page).setdefault('valid_proxy',False)

    while True:
        if get_page.valid_proxy == False:
            session.proxies.update(get_proxy())
            get_page.valid_proxy = True

        try:
            page = session.get(url, timeout=5)
            status_code = page.status_code
        except:
            print('Timeout of response...')
            get_page.valid_proxy = False
            continue
        if status_code != 200:
            print('Bad response')
            get_page.valid_proxy = False
            continue
        return page.text

# Получим данные по книге
def get_book_info(text):
    def get_value(text):
        return text.split(': ')[1].strip()

    t=a=y=g=r=e=''
    try:
        soup = BeautifulSoup(text, "html.parser")
        table = soup.find('table',{"id": "details"})
        cnt = table.find_all('tr')
        c=cnt[0].text.strip().split('\n')
        for i in c:
            if t=='' and 'Название' in i:
                t=get_value(i)
                continue
            if a=='' and 'Автор' in i:
                a=get_value(i)
                continue
            if y=='' and 'Год' in i:
                y=get_value(i)
                continue
            if g=='' and 'Жанр' in i:
                g=get_value(i)
                continue
            if r=='' and ('Исполнитель' in i) or ('Читает' in i):
                r=get_value(i)
                continue
            if e=='' and 'Продолжительность' in i:
                e=get_value(i)
                continue
        # Описание до первой пустой строки
        o_b=False
        o:str=''
        for i in c:
            if (o_b == False) and ('Описание' in i):
                o_b=True
                continue
            if o_b:
                if i=='':
                    break
                o=o+i.strip()

    except:
        return None
    return (t,a,y,g,r,e,o)



pg = 0
url = 'http://rutor.info/browse/{}/11/0/0'

session = requests.Session()

valid_proxy = False

while True:
    text = get_page(url.format(pg))

    try:
        soup = BeautifulSoup(text, "html.parser")
        table = soup.find("div", {"id": "index"}).find('table')
        rows = table.find_all("tr", class_=["gai", "tum"])
    except:
        print('HTML parse error...')
        time.sleep(5)
        continue

    # Список пуст? Значит последняя страница, выход...
    if len(rows) == 0:
        print('Done...')
        break

    for row in rows:
        td = row.find_all('td')
        nm = td[1].text.strip()
        if 'mp3' in nm.lower():
            links = td[1].find_all('a')
            magnet = links[1].attrs.get('href')
            aurl = links[2].attrs.get('href')
            if (aurl is None) or (magnet is None):
                continue

            aid = aurl.split('/')[2]
            # получим данные по книге
            text=get_page('http://rutor.info'+aurl)
            book_info = get_book_info(text)
            pass

            if book_info is None:
                print(f'Error parse book info:\n{nm}')
                continue
            book_info=(aid,nm,)+book_info+(aurl, magnet)
            rz = con.execute(sql_add, book_info)
            # update: если запись уже в БД - выходим (только при INSERT OR IGNORE)
            # if rz.rowcount == 0:
            #     print('Update complete...')
            #     exit(0)
            con.commit()
            print(f"{aid}\t{nm}")
            pass

    pg = pg + 1
