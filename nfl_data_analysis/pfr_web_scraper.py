import string
from bs4 import BeautifulSoup as bs
from bs4 import Comment
from urllib import request
import re
import pandas as pd
#import numpy as np
from collections import OrderedDict
from tqdm import tqdm
from joblib import Parallel, delayed
from itertools import repeat
from pickle import dumps
from multiprocessing import current_process


def comment_table_id(comment):
    html_wrap = '<html><body>{}</body></html>'
    try:
        out = bs(html_wrap.format(comment), 
                 'html.parser').find('table').attrs['id']
    except KeyError:
        try:
            out = bs(html_wrap.format(comment), 
                     'html.parser').find('table').attrs['class']
        except KeyError:
                out = str(bs(html_wrap.format(comment), 
                             'html.parser').find('table').attrs)
    return str(out)


#def get_player_pages(let, url_base):
#    base_page = request.urlopen(url_base + f'/players/{let}')
#    base_page_soup = bs(base_page, 'html.parser')
#    div_players = base_page_soup.find(attrs={'id': 'div_players'})
#    return div_players.find_all('a')
    

def main(let, url_base):
    process_num = current_process()
    html_wrap = '<html><body>{}</body></html>'
    all_players = {}
    base_page = request.urlopen(url_base + f'/players/{let}')
    base_page_soup = bs(base_page, 'html.parser')
    div_players = base_page_soup.find(attrs={'id': 'div_players'})
    players = div_players.find_all('a')
    desc=f'{process_num.name}\u2013{let} Players:'.ljust(30, '.')
    for player in tqdm(players, desc=desc):
        player_page = request.urlopen(url_base + player.attrs['href'])
        player_page_soup = bs(player_page, 'html.parser')
        comments = player_page_soup.find_all(
            string=lambda text:isinstance(text,Comment))
        comments = list(filter(lambda x: len(re.findall(r'<table', x)) == 1, 
                               comments))
        player_tables = [pd.read_html(html_wrap.format(str(x)))[0]
                         for x in comments]
        table_ids = [comment_table_id(x) for x in comments]
        all_players.update({player.parent.text:
                            dict(zip(table_ids, player_tables))})
        for table in player_page_soup.find_all('table'):
            table_df = None
            for row in table.find_all('tr'):
                table_row = OrderedDict()
                for td in row.find_all('td'):
                    if 'data-stat' in td.attrs:
                        if td.text.isnumeric():
                            td_num = float(td.text)
                            if td_num.is_integer():
                                td_num = int(td_num)
                            table_row.update({td.attrs['data-stat']: td_num})
                        elif td.text[:-1].isnumeric() and td.text[-1] == '%':
                            td_num = float(td.text[:-1])
                            if td_num.is_integer():
                                td_num = int(td_num)
                            table_row.update({td.attrs['data-stat']: td_num})
                        else:
                            table_row.update({td.attrs['data-stat']: td.text})
                table_row = pd.Series(table_row).to_frame().T
                if isinstance(table_df, type(None)):
                    table_df = table_row
                else:
                    table_df = table_df.append(table_row, ignore_index=True)
            all_players[player.parent.text].update(
                {table.attrs['id']: table_df[table_row.columns]})
    return all_players

if __name__ == '__main__':
    letters = string.ascii_uppercase
    url_base = 'https://www.pro-football-reference.com'
    iter_over = zip(letters, repeat(url_base))
    parallel = Parallel(n_jobs=8, verbose=15)
    parallel = Parallel(n_jobs=8)
    returned = parallel(delayed(main)(*x) for x in iter_over)
    
    out_dict = {k: x[k] for x in returned for k in x}
    out_path = '/home/bray/Documents/nfl_data_analysis/player_data_dict.pkl'
    with open(out_path, 'wb') as f:
        f.write(dumps(out_dict))
    
