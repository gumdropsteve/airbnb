import math
import time
import requests
import pandas as pd
import dask.delayed
from time import sleep
from dask import compute
from bs4 import BeautifulSoup
from datetime import date, datetime


def get_page(url):
    """
    returns a soup object that contains all the information of a given webpage
    """
    tos = str(datetime.now()) 
    result = requests.get(url)
    content = result.content
    page = BeautifulSoup(content, features='html')
    return page, tos


def get_room_classes(soup_page):
    """
    returns all the listings that can be found on the page (soup object) in a list
    """
    rooms = soup_page.findAll('div', {'class':'_8ssblpx'})  #  _8ssblpx _uhpzdny _gig1e7 _1wcpzyga
    result = []
    for room in rooms:
        result.append(room)
    return result


def get_listing_link(listing):
    """
    returns the URL link of given listing
    """
    listing_link = 'http://airbnb.com' + listing.find('a')['href']
    listing_link = listing_link.split('?')[0]
    return listing_link


def get_listing_title(listing):
    """
    returns the title of given listing
    """
    title = listing.find('meta')['content']
    title = title.split(' - null - ')
    return title[0]


def get_top_row(listing):
    """
    returns the top row of given listing's info
    """
    top_row = listing.find('div', {'class':'_1tanv1h'}).text  # _167gordg
    top_row = top_row.split(' in ')
    # what are we looking at?
    what_it_is = top_row[0]
    # where is it?
    where_it_is = top_row[1]
    return what_it_is, where_it_is


def get_room_info(listing):
    """
    returns room info of listing 
    """
    room_info = listing.find('div', {'class', '_kqh46o'}).text
    split_info = [i.split() for i in room_info.split(' · ')]
    room_dict = {}
    for i in split_info:
        if i not in [['Studio'], ['Half-bath']]:
            if len(i) == 2:
                room_dict[i[1]] = i[0]
            # shared-baths
            elif len(i) == 3:
                i = [i[0], '-'.join([i[1], i[2]])]
                room_dict[i[1]] = i[0]
            else:
                if i[1] == 'total':
                    room_dict['bedrooms'] = [i[0]]
                else:
                    print(f'unexpected room_info | unexpected split_info len(i)=={len(i)}!=2!=3\n{i}')
                    room_dict[' '.join(i)] = i[0]
        else:
            # Half-baths and Studios
            if i[0] == 'Studio':
                room_dict['is_studio'] = True
            room_dict[i[0]] = 0
    
    # need better solution for bedrooms
    weird_bedrooms = 0 
    try:
        b = room_dict['bedrooms']
        del b
    except:
        try:
            room_dict['bedrooms'] = room_dict['bedroom']
        except:
            try:
                room_dict['bedrooms'] = room_dict['Studio']
            except:
                weird_bedrooms += 1
                print(f'weird bedrooms {weird_bedrooms}')
                room_dict['bedrooms'] = room_dict.get('bedrooms')
    
    try:
        room_dict['baths']
    except:
        try:
            room_dict['baths'] = room_dict['bath']
        except:
            room_dict['baths'] = None
    
    room_dict['half_baths'] = room_dict.get('Half-bath')
    room_dict['shared_baths'] = room_dict.get('shared-baths')
    room_dict['is_studio'] = room_dict.get('is_studio', False)
    room_dict['beds'] = room_dict.get('beds')
    room_dict['guests'] = room_dict.get('beds')

    # check for bedrooms list
    if type(room_dict['bedrooms']) == list:
        if len(room_dict['bedrooms']) == 1:
            room_dict['bedrooms'] = float(room_dict['bedrooms'][0])
        else:
            raise Exception(f'unexpected bedrooms list | {room_dict["bedrooms"]}')
            
    room_dict = {key:value for key,value in room_dict.items() if key in ['guests', 'bedrooms', 'beds', 'is_studio', 'baths', 'half_baths', 'shared_baths']}
            
    return room_dict


def get_room_price(listing):
    """
    returns the nightly rate (price) of given listing
    """
    price_text = listing.find('div', {'class':'_ls0e43'}).text
    price = price_text.split('$')
    price = price[1]
    # extract float value
    price = price.split(" ")[0]  # skip the $
    # remove possible / at end of string
    if '/' in price:
        price = price[:len(price) - 1]
    # adjust for places with > 999 reviews
    if ',' in price:
        price = ''.join(price.split(','))
    return float(price)


def get_room_rating_and_reviews(listing):
    """
    returns star rating and number of reviews of given listing
    """
    try:
        output = listing.find('span', {'class':'_18khxk1'}).text
        output = output.split('\xa0')
        
        avg_rating = float(output[0])
        n_reviews = float(output[1][:-1].split('(')[1])

        return avg_rating, n_reviews
    except:
        try:
            return listing.find('span', {'class':'_18khxk1'}), listing.find('span', {'class':'_18khxk1'})
        except:
            raise Exception(f'get_room_rating_and_reviews | listing == {type(listing), len(listing)}')


class airbnb_scrape():
    
    def __init__(self, location, location_alias):
        """
        set location, base (url) link, and blank record books
        """
        self.base_link = f'http://www.airbnb.com/s/{location}/homes'
        self.location = location
        self.location_alias = location_alias
        
        self.n_pages = None
        self.n_results = None
        self.page_urls = []
        self.data_dir = 'data/'
        
        # set known basic amenities
        self.possible = ['Gym', 'Wifi', 'Self check-in', 'Air conditioning', 'Pets allowed', 'Indoor fireplace', 'Hot tub', 'Free parking', 'Pool', 'Kitchen', 'Breakfast', 'Elevator', 'Washer', 'Dryer', 
                         'Heating', 'Waterfront', 'Dishwasher', 'Beachfront', 'Ski-in/Ski-out', 'Terrace', 'Sonos sound system', 'BBQ grill', 'Hair dryer', "Chef's kitchen", 'Wet bar', 'Sun loungers', 
                         'Home theater', 'Housekeeping', 'Gated property', 'Gas fireplace', 'Plunge pool', 'Infinity pool', 'Sun deck', 'Game room', 'Surround sound system', 'Resort access']

        # set current schema column names
        self.names = ['ds', 'search_filter', 'url', 'title', 'type', 'location', 'guests', 'bedrooms', 'beds', 'is_studio', 'baths', 'half_baths', 'shared_baths', 'price', 'avg_rating', 'n_reviews', 'gym_bool', 
                      'wifi_bool', 'self_check_in_bool', 'air_conditioning_bool', 'pets_allowed_bool', 'indoor_fireplace_bool', 'hot_tub_bool', 'free_parking_bool', 'pool_bool', 'kitchen_bool', 'breakfast_bool', 
                      'elevator_bool', 'washer_bool', 'dryer_bool', 'heating_bool', 'waterfront_bool', 'dishwasher_bool', 'beachfront_bool', 'ski_in_ski_out_bool', 'terrace_bool', 'sonos_sound_system_bool', 
                      'bbq_grill_bool', 'hair_dryer_bool', 'chefs_kitchen_bool', 'wet_bar_bool', 'sun_loungers_bool', 'home_theater_bool', 'housekeeping_bool', 'gated_property_bool', 'gas_fireplace_bool', 
                      'plunge_pool_bool', 'infinity_pool_bool', 'sun_deck_bool', 'game_room_bool', 'surround_sound_system_bool', 'resort_access_bool']
        
        self.dtypes = {'ds': 'object', 'search_filter': 'object', 'url': 'object', 'title': 'object', 'type': 'object', 'location': 'object', 'guests': 'float64', 'bedrooms': 'float64', 'beds': 'float64', 
                       'is_studio': 'bool', 'baths': 'float64', 'half_baths': 'float64', 'shared_baths': 'float64', 'price': 'float64', 'avg_rating': 'float64', 'n_reviews': 'float64', 'gym_bool': 'bool', 
                       'wifi_bool': 'bool', 'self_check_in_bool': 'bool', 'air_conditioning_bool': 'bool', 'pets_allowed_bool': 'bool', 'indoor_fireplace_bool': 'bool', 'hot_tub_bool': 'bool', 'free_parking_bool': 
                       'bool', 'pool_bool': 'bool', 'kitchen_bool': 'bool', 'breakfast_bool': 'bool', 'elevator_bool': 'bool', 'washer_bool': 'bool', 'dryer_bool': 'bool', 'heating_bool': 'bool', 
                       'waterfront_bool': 'bool', 'dishwasher_bool': 'bool', 'beachfront_bool': 'bool', 'ski_in_ski_out_bool': 'bool', 'terrace_bool': 'bool', 'sonos_sound_system_bool': 'bool', 
                       'bbq_grill_bool': 'bool', 'hair_dryer_bool': 'bool', 'chefs_kitchen_bool': 'bool', 'wet_bar_bool': 'bool', 'sun_loungers_bool': 'bool', 'home_theater_bool': 'bool', 'housekeeping_bool': 'bool', 
                       'gated_property_bool': 'bool', 'gas_fireplace_bool': 'bool', 'plunge_pool_bool': 'bool', 'infinity_pool_bool': 'bool', 'sun_deck_bool': 'bool', 'game_room_bool': 'bool', 
                       'surround_sound_system_bool': 'bool', 'resort_access_bool': 'bool'}

    def get_basic_facilities(self, listing):
        '''
        returns a dictionary of the given listing's basic facilities with True / None values based on known possible basic facilites
        '''
        # make list of this listing's basic facilites
        try:
            basic_facilities = listing.findAll("div", {"class":"_kqh46o"})[1].text
            basic_facilities = basic_facilities.split(' · ')
        except:
            basic_facilities = []

        # open a record for this listing
        room_dict = {}
        
        # add each basic facility to this room's record 
        for f in basic_facilities:
            if f in self.possible:
                room_dict[f] = True
            else:
                # looks liek we have a new basic facility
                i = input(f'unexpected basic_facilites | {f} | is new? (y/n) ')
                if i == 'y':
                    i = input(f'ok, new basic facility\nwhat should the column name be?\ne.g. Hot tub is hot_tub_bool\n"exit" to quit\n column name == ')
                    if i != 'exit':
                        # set new amenity
                        room_dict[f] = True
                        # update possible amenities and column names
                        self.possible.append(f)
                        self.names.append(i)
                        print(f'\nnew self.possible ==\n{self.possible}\n\nnew self.names ==\n{self.names}\n\nplease update now (sleeping 60 seconds)\n')
                        sleep(60)
                    else:
                        raise Exception(f"not sure what's going on.. | unexpected basic_facilites | {f} | user exit")
                else:
                    raise Exception(f"not sure what's going on.. | unexpected basic_facilites | {f}")
        
        # add None for any basic facilities this listing doesn't offer
        for f in self.possible:
            room_dict[f] = room_dict.get(f, None)
        
        return room_dict
    
    def find_n_results(self, soup_page):
        """
        finds total number of search results from page 1 (of search results)
        """
        try:
            # keep track of how many results we have
            self.n_results = soup_page.find('div', {'class':'_1h559tl'}).text
        except:
            raise Exception('n results not found on 1st page')
    
    def find_n_pages(self, soup_page, listings_per_page=20):
        """
        finds number of existing pages from 1st page of search results
        """
        try:
            n_results_string = soup_page.find('div', {'class':'_1h559tl'}).text 
            # check if 300+ club
            if '300+' in n_results_string:
                self.n_pages = 15
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
                    self.n_pages = n_pages
        except:
            print(f'find_n_pages error | {self.location}')
            self.n_pages = 1
        # tell me how many pages there are
        print(self.n_pages)
    
    def make_page_urls(self, base_page, n_pages='auto', listings_per_page=20):
        """
        makes pages for search results (sets of 20)
        """
        # reset page urls
        self.page_urls = []
        # if n_pages wasn't set
        if n_pages == 'auto':
            # find out how many pages there are
            self.find_n_pages(base_page, listings_per_page=listings_per_page)
        # items_offset is 1st filter (?) or after 1st filter (&)
        if '?' not in base_page:
            c = '?'
        else:
            c = '&'
        # create page urls
        for i in range(self.n_pages):
            # 1st page alread done earlier
            if i != 0:
                url = f'{base_page}{c}items_offset={i * listings_per_page}'
                self.page_urls.append(url)
            else:
                pass
    
    def record_dataset(self, listings, tos, _filter):
        """
        take scraped room classes and record their information to csv

        tos: time of scrape
            > str datetime.datetime.now()

        _filter: filter applied to scrape
            > str, None if no filter
        """
        data = []
        for l in listings:
            # listing link
            a = get_listing_link(l)
            # listing title
            b = get_listing_title(l)
            # top row info
            c, d = get_top_row(l)
            # room info (beds, baths, etc..)
            _ = get_room_info(l)
            e, f, g, h, i, j, k = _['guests'], _['bedrooms'], _['beds'], _['is_studio'], _['baths'], _['half_baths'], _['shared_baths']
            del _
            # room nightly rate
            m = get_room_price(l)
            # room rating and n reviews
            n, o = get_room_rating_and_reviews(l)
            # basic facilites
            _ = self.get_basic_facilities(l)
            p = [_[bf] for bf in self.possible]
            # list of all listing info
            out = [_filter] + [a, b, c, d, e, f, g, h, i, j, k, m, n, o] + p
            # add time of scrape to data as 1st datapoint (jan 15 2021)
            out = [tos] + out
            # add it to the data collection 
            data.append(out)
        
        # add this scrape to the location's existing dataset
        try:
            pd.concat([pd.read_parquet(f'{self.data_dir}{self.location_alias}.parquet'), 
                       pd.DataFrame(data, columns=self.names)], axis=0).to_parquet(f'{self.data_dir}{self.location_alias}.parquet', index=False)
        # first time we've scraped this location, make a new dataset
        except:
            # check this is actually new so we don't accidenly overwrite existing data (delete 'y'# from the below line if you want to perform manual check, outherwise defaults to make new file)
            i = 'y'#input(f'recording new location: {self.location_alias}? (y/n) ')
            if i == 'y':
                # make dataframe from scraped data, column names from __init__()
                df = pd.DataFrame(data, columns=self.names)
                # go through each column
                for column in self.dtypes:
                    # our bool data is scraped as True/None, we need True/False
                    if 'bool' in column:
                        # fill None values in bool column with False
                        df[column] = df[column].fillna(False)
                    # convert column to expected dtype for parquet
                    df[column] = df[column].astype(self.dtypes[column])
                # write new parquet file
                df.to_parquet(f'{self.data_dir}{self.location_alias}.parquet', index=False)
                del df  # free up space
            else:
                raise Exception("not recording a new location, what's going on?")
    
    def scrape_search(self, base_link, search_alias, _filter, n_pages='auto', printout=False):
        """
        record results of a given search link
        """        
        # get 1st page
        base_link_page_1, t = get_page(base_link)
        
        # record the 1st page
        if printout:
            print(self.record_dataset(get_room_classes(base_link_page_1), tos=t, _filter=_filter))
        else:
            self.record_dataset(get_room_classes(base_link_page_1), tos=t, _filter=_filter)
        
        # get urls for other pages 
        if n_pages=='auto':
            self.make_page_urls(self.base_link, self.find_n_pages(base_link_page_1))
        else:
            self.make_page_urls(self.base_link, n_pages)        
        
        for url in self.page_urls:
            if printout:
                page, t = get_page(url)
                print(self.record_dataset(get_room_classes(page), tos=t, _filter=_filter))
            else:
                page, t = get_page(url)
                self.record_dataset(get_room_classes(page), tos=t, _filter=_filter)
                
        # output where we can find the file (relative path)
        return f'{self.data_dir}{self.location_alias}.parquet'
    
    @dask.delayed
    def scrape_types(self, printout=False):
        """
        record data from a loacations results for each of the big 4 room type filters and for each of those with superhosts only filter applied (8 total)
        """
        print(f'starting {self.location.split("--")[0]} @ {self.base_link}')  # scrape all 4 room types (default and with superhost filter)
        
        today = str(date.today())
        try:
            last_date_recorded = pd.read_parquet(f'{self.data_dir}{self.location_alias}.parquet').ds.str.split()[-1:].values[0][0]
        except:
            last_date_recorded = None
            
        # check to make sure we haven't already recorded this place today
        if last_date_recorded != today:
            # default search
            self.scrape_search(self.base_link, f'{self.location_alias}', _filter='', printout=printout)
            self.scrape_search(f'{self.base_link}?superhost=true', f'{self.location_alias}_super_hosts', _filter='super_hosts', printout=printout)

            # entire homes only
            self.scrape_search(f'{self.base_link}?room_types[]=Entire home', f'{self.location_alias}_entire_homes', _filter='entire_homes', printout=printout) 
            self.scrape_search(f'{self.base_link}?room_types[]=Entire home&superhost=true', f'{self.location_alias}_entire_home_super_hosts', _filter='entire_home_super_hosts', printout=printout)

            # hotes rooms only
            self.scrape_search(f'{self.base_link}?room_types[]=Hotel room', f'{self.location_alias}_hotel_rooms', _filter='hotel_rooms', printout=printout)
            self.scrape_search(f'{self.base_link}?room_types[]=Hotel room&superhost=true', f'{self.location_alias}_hotel_room_super_hosts', _filter='hotel_room_super_hosts', printout=printout)

            # private rooms only
            self.scrape_search(f'{self.base_link}?room_types[]=Private room', f'{self.location_alias}_private_rooms', _filter='private_rooms', printout=printout)
            self.scrape_search(f'{self.base_link}?room_types[]=Shared room&superhost=true', f'{self.location_alias}_private_room_super_hosts', _filter='private_room_super_hosts', printout=printout)

            # shared rooms only
            self.scrape_search(f'{self.base_link}?room_types[]=Private room', f'{self.location_alias}_shared_rooms', _filter='shared_rooms', printout=printout)
            self.scrape_search(f'{self.base_link}?room_types[]=Shared room&superhost=true', f'{self.location_alias}_shared_room_super_hosts', _filter='shared_room_super_hosts', printout=printout)
        # we already recorded today
        else:
            print(f'{self.location.split("--")[0]} already recorded today')


if __name__=='__main__':
    from where_are_you_going import locations, location_aliases
    
    # start timer
    start = time.time()

    # add each delayed location to a collection for delayed (parallel) scrape
    collection = []
    for _ in range(len(locations)):
        # make airbnb scrape class instance for this location
        l = airbnb_scrape(location=locations[_], location_alias=location_aliases[_])
        
        # make delayed scrape_types() method for this location
        delayed_scrape = dask.delayed(l.scrape_types)(l, printout=False)

        collection.append(delayed_scrape)

    # execute delayed scrapes
    compute(*collection)

    print(f'runtime: {time.time() - start}')
