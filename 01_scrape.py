import math 
import requests
import pandas as pd
import dask.delayed
from dask import compute
from bs4 import BeautifulSoup
from datetime import datetime


def get_page(url):
    """
    returns a soup object that contains all the information of a given webpage
    """
    result = requests.get(url)
    content = result.content
    return BeautifulSoup(content, features='html.parser')


def get_room_classes(soup_page):
    """
    returns all the listings that can be found on the page (soup object) in a list
    """
    rooms = soup_page.findAll('div', {'class':'_8ssblpx'})
    result = []
    for room in rooms:
        result.append(room)
    return result


def get_listing_link(listing):
    """
    returns the URL link of given listing
    """
    listing_link = 'https://airbnb.com' + listing.find('a')['href']
    listing_link = listing_link.split('?')[0]
    return listing_link


def get_listing_title(listing):
    """
    returns the title of given listing
    """
    return listing.find('meta')['content']


def get_top_row(listing):
    """
    returns the top row of given listing's info
    """
    return listing.find('div', {'class':'_1tanv1h'}).text  # _167gordg


def get_room_info(listing):
    """
    returns room info of listing 
    """
    return listing.find('div', {'class', '_kqh46o'}).text


def get_room_price(listing):
    """
    returns the nightly rate (price) of given listing
    """
    price_text = listing.find('div', {'class':'_ls0e43'}).text
    price = price_text.split('Price:')
    return price[1]


def get_basic_facilities(listing):
    ''' Returns the basic facilities'''
    try:
        output = listing.findAll("div", {"class":"_kqh46o"})[1].text.replace(" ","") #Speeds up cleaning
    except:
        output = []
    return output


def get_room_rating(listing):
    """
    returns star rating of given listing
    """
    try:
        return listing.find('div', {'class':'_vaj62s'}).text
    except:
        return listing.find('div', {'class':'_vaj62s'})

    
def get_n_reviews(listing):
    '''
    Returns the number of reviews
    '''
    try:  # Not all listings have reviews // extraction failed
        output = listing.findAll("span", {"class":"_krjbj"})[1].text
    except:
        output = None   # Indicate that the extraction failed -> can indicate no reviews or a mistake in scraping
    return output


def record_dataset(listings, file_path='output.csv', first_page=False):
    """
    take scraped room classes and record their information to csv
    """
    data = []
    for l in listings:
        a = get_listing_link(l)
        b = get_listing_title(l)
        c = get_top_row(l)
        d = get_room_info(l)
        e = get_room_price(l)
        f = get_basic_facilities(l)
        g = get_room_rating(l)
        h = get_n_reviews(l)
        out = [a, b, c, d, e, f, g, h]
        data.append(out)
    if first_page:
        names = [l for l in 'abcdefgh']
        df = pd.DataFrame(data, columns=names)
    else:
        df = pd.read_csv(file_path)
        names = df.columns
        new_df = pd.DataFrame(data, columns=names)
        df = pd.concat([df, new_df], axis=0)
    df.to_csv(file_path, index=False)
    return len(df)


def find_n_results(soup_page):
    """
    returns total number of search results from page 1 (of search results)
    """
    return soup_page.find('div', {'class':'_1h559tl'}).text


def find_n_pages(soup_page, listings_per_page=20):
    """
    returns number of existing pages from 1st page of search results
    """
    n_results_string = soup_page.find('div', {'class':'_1h559tl'}).text
    # check if 300+ club
    if '300+' in n_results_string:
        n_pages = 15
    else:
        split_results_string = n_results_string.split(' of ')
        n_total_results_string = split_results_string[1]
        # check for unknown + edge case
        if '+' in n_total_results_string:
            raise Exception(f'+ in n_total_results_string but 300+ is not\nn_total_results_string == {n_total_results_string}')
        else:
            # find number of results
            split_total_results_string = n_total_results_string.split(' ')
            n_total_results = int(split_total_results_string[0])
            n_pages = n_total_results / listings_per_page 
            n_pages = math.ceil(n_pages)
    # tell me how many pages there are
    return n_pages


def make_page_urls(base_page, n_pages, listings_per_page=20):
    """
    makes pages for search results (sets of 20)
    """
    if '?' not in base_page:
        c = '?'
    else:
        c = '&'
    page_urls = []
    for i in range(n_pages):
        if i != 0:
            url = f'{base_page}{c}items_offset={i * listings_per_page}'
            page_urls.append(url)
        else:
            pass
    return page_urls


def scrape_search(base_link, search_alias, n_pages='auto', printout=False):
    """
    record results of a given search link
    """
    # get 1st page
    base_link_page_1 = get_page(base_link)
    
    today = datetime.today()
    today = str(today).split(' ')[0]
    output_path = f'{search_alias}_{today}.csv'
    
    # record the 1st page
    r = record_dataset(get_room_classes(base_link_page_1), file_path=output_path, first_page=True)
    if printout:
        print(r)
    else:
        r

    # get urls for other pages 
    if n_pages=='auto':
        page_urls = make_page_urls(base_link, find_n_pages(base_link_page_1))
    else:
        page_urls = make_page_urls(base_link, n_pages)        

    for url in page_urls:
        r = record_dataset(get_room_classes(get_page(url)), file_path=output_path, first_page=False)
        if printout:
            print(r)
        else:
            r


@dask.delayed
def scrape_types(location='Las-Vegas--NV--United-States', location_alias='las_vegas', printout=False):
    """
    record data from a loacations results for each of the big 4 room type filters and for each of those with superhosts only filter applied (8 total)
    """
    
    base_link = f'https://www.airbnb.com/s/{location}/homes'
    
    print(f'starting {location.split("--")[0]} @ {base_link}')

    entire_homes_link = f'https://www.airbnb.com/s/{location}/homes?room_types[]=Entire home'
    entire_home_super_hosts_link = f'https://www.airbnb.com/s/{location}/homes?room_types[]=Entire home&superhost=true'

    hotel_rooms_link = f'https://www.airbnb.com/s/{location}/homes?room_types[]=Hotel room'
    hotel_room_super_hosts_link = f'https://www.airbnb.com/s/{location}/homes?room_types[]=Hotel room&superhost=true'

    private_rooms_link = f'https://www.airbnb.com/s/{location}/homes?room_types[]=Private room'
    private_room_super_hosts_link = f'https://www.airbnb.com/s/{location}/homes?room_types[]=Shared room&superhost=true'

    shared_rooms_link = f'https://www.airbnb.com/s/{location}/homes?room_types[]=Private room'
    shared_room_super_hosts_link = f'https://www.airbnb.com/s/{location}/homes?room_types[]=Shared room&superhost=true'

    super_hosts_link = f'https://www.airbnb.com/s/{location}/homes?superhost=true'

    # default search (+ only superhost)
    scrape_search(base_link, f'{location_alias}', printout=printout)
    scrape_search(super_hosts_link, f'{location_alias}_super_hosts', printout=printout)
    
    # entire homes only (+ only superhost)
    scrape_search(entire_homes_link, f'{location_alias}_entire_homes', printout=printout) 
    scrape_search(entire_home_super_hosts_link, f'{location_alias}_entire_home_super_hosts', printout=printout)
    
    # hotes rooms only (+ only superhost)
    scrape_search(hotel_rooms_link, f'{location_alias}_hotel_rooms', printout=printout)
    scrape_search(hotel_room_super_hosts_link, f'{location_alias}_hotel_room_super_hosts', printout=printout)
    
    # private rooms only (+ only superhost)
    scrape_search(private_rooms_link, f'{location_alias}_private_rooms', printout=printout)
    scrape_search(private_room_super_hosts_link, f'{location_alias}_private_room_super_hosts', printout=printout)
    
    # shared rooms only (+ only superhost)
    scrape_search(shared_rooms_link, f'{location_alias}_shared_rooms', printout=printout)
    scrape_search(shared_room_super_hosts_link, f'{location_alias}_shared_room_super_hosts', printout=printout)
    
    
locations = ['Oakland--California--United-States',
             'San-Diego--California--United-States',
             'San-Francisco--California--United-States',
             'California--United-States',
             
             'Bentonville--Arkansas--United-States',
             'Bella-Vista--Arkansas--United-States',
             'Little-Rock--Arkansas--United-States',
             'Arkansas--United-States',
             
             'Austin--Texas--United-States',
             'Dallas--Texas--United-States',
             'Houston--Texas--United-States',
             'Texas--United-States',
             
             'Paradise--Nevada--United-States',
             'Henderson--Nevada--United-States',
             'Reno--Nevada--United-States',
             'Nevada--United-States',
             
             'Anchorage--Alaska--United-States',
             'North-Pole--Alaska--United-States',
             'Alaska--United-States']

location_aliases = ['oakland',
                    'san_diego',
                    'san_francisco',
                    'california',
                    
                    'bentonville',
                    'bella_vista',
                    'little_rock',
                    'arkansas',
                    
                    'austin',
                    'dallas',
                    'houston',
                    'texas',
                    
                    'paradise',
                    'henderson',
                    'reno',
                    'nevada',
                    
                    'anchorage',
                    'north_pole',
                    'alaska']

collection = []
for _ in range(len(locations)):
    l = dask.delayed(scrape_types)(locations[_], location_aliases[_])
    collection.append(l)
    
compute(*collection)
