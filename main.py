import format_pb2 as pb
import upstox_client
from datetime import datetime, time, timedelta
from upstox_client.rest import ApiException
from pprint import pprint
import safety #safety.py module, which consists of our credentials
import asyncio, websockets
import json, os, sys, ssl
from google.protobuf.json_format import MessageToDict
import xlwings as xw
import pandas as pd
from py_vollib.black_scholes.greeks.analytical import delta, gamma, rho, theta, vega
from concurrent.futures import ThreadPoolExecutor, wait
import re
from copy import deepcopy

# Configure OAuth2 access token for authorization: OAUTH2
configuration = upstox_client.Configuration()
configuration.access_token = safety.token

# create an instance of the API class
api_instance = upstox_client.WebsocketApi(upstox_client.ApiClient(configuration))
api_version = '2.0'

if not os.path.exists("Real-Time Ticker.xlsm"):
    try:
        book = xw.Book()
        book.save("Real-Time Ticker.xlsm")
        book.close()
    except Exception as e:
        print(f"Error Creating Excel File : {e}")
        sys.exit()
book = xw.Book("Real-Time Ticker.xlsm")

sheet_no = 1


sheet_exists = False
for ex_sheet in book.sheets:
    if ex_sheet.name == f"OptionChain{sheet_no}":
        ex_sheet.range("a1:af22").value = None
        sheet_exists = True
        break

if not sheet_exists:
    book.sheets.add(f"OptionChain{sheet_no}")
    book.save()

sheet = book.sheets(f"OptionChain{sheet_no}")

def scraping_data_to_excel(response, strike_type, current_price, expiry_date):
    index_dict = {
        "ltp" : "j" if strike_type == "CE" else "v",
        "chng" : "k" if strike_type == "CE" else "u",
        "rho" : "e" if strike_type == "CE" else "aa",
        "iv" : "i" if strike_type == "CE" else "w",
        "delta" : "a" if strike_type == "CE" else "ae",
        "theta" : "c" if strike_type == "CE" else "ac",
        "gamma" : "b" if strike_type == "CE" else "ad",
        "vega" : "d" if strike_type == "CE" else "ab",
        "volume" : "h" if strike_type == "CE" else "x",
        "oi" : "f" if strike_type == "CE" else "z",
        "chng_oi" : "g" if strike_type == "CE" else "y"
    }

    count = 3
    for feed in response.values():
        ff = feed.get('ff').get("marketFF")
        #ltp and chng
        temp = ff.get("ltpc")
        print(temp.get("ltp"))
        sheet.range(f'{index_dict["ltp"]}{count}').value = temp.get("ltp")
        sheet.range(f'{index_dict["chng"]}{count}').value = temp.get("ltp") - temp.get("cp")

        #option_greeks
        S = current_price  
        K = feed.get("price")  
        t = ((datetime(expiry_date.year, expiry_date.month, expiry_date.day, 15, 30) - datetime.now()) / timedelta(days=1)) / 365
        r = 0.03
        option_type = 'p' if strike_type.upper() == 'PE' else 'c'

        temp = ff.get('optionGreeks')
        sigma = sheet.range(f'{index_dict["iv"]}{count}').value = temp.get("iv") if temp else 0
        sheet.range(f'{index_dict["rho"]}{count}').value= rho(option_type, S, K, t, r, sigma)

        sheet.range(f'{index_dict["delta"]}{count}').value = temp.get("delta") if temp else 0
        sheet.range(f'{index_dict["theta"]}{count}').value = temp.get("theta") if temp else 0
        sheet.range(f'{index_dict["gamma"]}{count}').value = temp.get("gamma") if temp else 0
        sheet.range(f'{index_dict["vega"]}{count}').value = temp.get("vega") if temp else 0

        temp = ff.get("eFeedDetails")
        sheet.range(f'{index_dict["volume"]}{count}').value = temp.get("vtt") if temp else 0 
        sheet.range(f'{index_dict["oi"]}{count}').value = temp.get("oi")  if temp else 0
        sheet.range(f'{index_dict["chng_oi"]}{count}').value = temp.get("oi") - temp.get("poi")  if temp else 0
        book.save("Real-Time Ticker.xlsm")
        count += 1





def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(
        upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response


def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


async def fetch_market_data(instrument_dict, price, expiry_date):
    """Fetch market data using WebSocket and print it."""

    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Configure OAuth2 access token for authorization
    configuration = upstox_client.Configuration()

    api_version = '2.0'
    configuration.access_token = safety.token

    # Get market data feed authorization
    response = get_market_data_feed_authorize(
        api_version, configuration)

    # Connect to the WebSocket with SSL context
    async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:
        print('Connection established')

        await asyncio.sleep(1)  # Wait for 1 second

        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": [x for x in instrument_dict.keys()]
            }
        }

        # Convert data to binary and send over WebSocket
        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)

        pe_pattern = r'\D+(\d+)PE'
        ce_pattern = r'\D+(\d+)CE'
        
        # Continuously receive and decode data from WebSocket

        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)

            # Convert the decoded data to a dictionary
            data_dict = MessageToDict(decoded_data)
            pe_dict = dict()
            ce_dict = dict()
            for x in data_dict['feeds'].keys():
                if "CE" in instrument_dict[x]:
                    match = re.search(ce_pattern, instrument_dict[x])
                    if match:
                        data_dict["feeds"][x]['price'] = int(match.group(1))
                    ce_dict[x] = data_dict["feeds"][x]

                else:
                    match = re.search(pe_pattern, instrument_dict[x])
                    if match:
                        data_dict["feeds"][x]['price'] = int(match.group(1))
                    pe_dict[x] = data_dict["feeds"][x]

            ce_dict = dict(sorted(ce_dict.items(), key=lambda x: x[1]['price']))
            pe_dict = dict(sorted(pe_dict.items(), key=lambda x: x[1]['price']))
            # with ThreadPoolExecutor() as executor:
            #     future1 = executor.submit(scraping_data_to_excel,ce_dict, "CE", price, expiry_date)
            #     future2 = executor.submit(scraping_data_to_excel,pe_dict, "PE", price, expiry_date)

            #     wait([future1, future2])
          
            scraping_data_to_excel(ce_dict, "CE", price, expiry_date)
            scraping_data_to_excel(pe_dict, "PE", price, expiry_date)
            print("threads done")




if __name__ == "__main__":


    dtype = {'instrument_key': str, 'tradingsymbol': str}
    df = pd.read_csv('NSE.csv', usecols=['instrument_key', 'tradingsymbol'],dtype=dtype)
    df.dropna(subset=['tradingsymbol'], inplace=True)

    #TODO how to take input, figure out.
    #Taking date as input and Parsing it.
    input_date = "2023-09-23"
    date_obj = datetime.strptime(input_date, "%Y-%m-%d")

    expiry_date = date_obj.strftime("%d%b").upper()

    call_df = deepcopy(df[df['tradingsymbol'].str.contains(rf'^BANKNIFTY{expiry_date}.*CE', case=True, regex=True)]).reset_index(drop=True)
    put_df = deepcopy(df[df['tradingsymbol'].str.contains(rf'^BANKNIFTY{expiry_date}.*PE', case=True, regex=True)]).reset_index(drop=True)

    # filtered_column = call_df['tradingsymbol']
    # filtered_column.to_csv('call.csv', index=False, header=['Symbols'])
    # filtered_column = put_df['tradingsymbol']
    # filtered_column.to_csv('put.csv', index=False, header=['Symbols'])


    configuration = upstox_client.Configuration()
    configuration.access_token = safety.token

    api_instance = upstox_client.MarketQuoteApi(upstox_client.ApiClient(configuration))
    symbol = 'NSE_INDEX|Nifty Bank'
    api_version = '2.0'

    current_price = 0
    try:
        api_response = api_instance.get_full_market_quote(symbol, api_version)
        symbol = symbol.replace('|',':')
        current_price = api_response.data[symbol].last_price
    except ApiException as e:
        print("Exception when calling MarketQuoteApi->get_full_market_quote: %s\n" % e)
    x = 0
    rounded_price = round(current_price / 50) * 50
    spot_price = 0
    tradingsymbol = ""
    while True:
        spot_price = rounded_price - (x*50)
        flag = call_df['tradingsymbol'].str.contains(f'BANKNIFTY{expiry_date}{spot_price}CE', case=False).any()
        if flag:
            tradingsymbol = f'BANKNIFTY{expiry_date}{spot_price}CE'
            break
        spot_price = rounded_price + (x*50)
        flag = call_df['tradingsymbol'].str.contains(f'^BANKNIFTY{expiry_date}{spot_price}CE', case=False).any()
        if flag:
            tradingsymbol = f'BANKNIFTY{expiry_date}{spot_price}CE'
            break
        x += 1
    
    #Creating Call Dict around Spot Price
    target_row_index = call_df[call_df['tradingsymbol'] == tradingsymbol].index[0]
    start_index = max(target_row_index - 8, 0) 
    end_index = min(target_row_index + 9, len(call_df)) 
    selected_rows = call_df.iloc[start_index:end_index]
    ce_dict = { row['instrument_key']: row['tradingsymbol'] for index, row in selected_rows.iterrows()}

    #Creating Put Dict around Spot Price
    tradingsymbol = tradingsymbol.replace("CE","PE")
    target_row_index = put_df[put_df['tradingsymbol'] == tradingsymbol].index[0]
    start_index = max(target_row_index - 8, 0) 
    end_index = min(target_row_index + 9, len(put_df))
    selected_rows = put_df.iloc[start_index:end_index]
    pe_dict = { row['instrument_key']: row['tradingsymbol'] for index, row in selected_rows.iterrows()}
    pe_dict.update(ce_dict)
    
    asyncio.run(fetch_market_data(pe_dict,current_price,date_obj))


