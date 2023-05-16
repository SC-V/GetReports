import datetime
import requests
import json
import pandas
import numpy
import io
import time
import re
import haversine as hs
from pytz import timezone
from googleapiclient import discovery
import streamlit as st
import streamlit_analytics
import pydeck as pdk

st.set_page_config(layout="wide")

FILE_BUFFER = io.BytesIO()
DEFAULT_CLAIM_SECRET = st.secrets["CLAIM_SECRET"]
CLAIM_SECRETS = st.secrets["CLAIM_SECRETS"]
SHEET_KEY = st.secrets["SHEET_KEY"]
SHEET_ID = st.secrets["SHEET_ID"]
COD_SHEET_KEY = st.secrets["COD_SHEET_KEY"]
COD_SHEET_ID = st.secrets["COD_SHEET_ID"]
API_URL = st.secrets["API_URL"]
SECRETS_MAP = {"Petco": 0,
               "Pets Table": 1,
               "Huevos": 2,
               "Inkovsky": 3,
               "Baby Creisy": 4,
               "Vigilancia Network": 5,
               "Lens Market": 6,
               "Ebebek": 7,
               "Supplementer": 8,
               "Sadece-eczane": 9,
               "Osevio Internet Hizmetleri": 10,
               "Mevsimi": 11,
               "Candy Gift": 12,
               "Akel": 13,
               "Espresso Perfetto": 14,
               "Ceviz Agaci": 15,
               "Guven Sanat": 16}

statuses = {
    'delivered': {'type': '4. delivered', 'state': 'in progress'},
    'pickuped': {'type': '3. pickuped', 'state': 'in progress'},
    'returning': {'type': '3. pickuped', 'state': 'in progress'},
    'cancelled_by_taxi': {'type': 'X. cancelled', 'state': 'final'},
    'delivery_arrived': {'type': '3. pickuped', 'state': 'in progress'},
    'cancelled': {'type': 'X. cancelled', 'state': 'final'},
    'performer_lookup': {'type': '1. created', 'state': 'in progress'},
    'performer_found': {'type': '2. assigned', 'state': 'in progress'},
    'performer_draft': {'type': '1. created', 'state': 'in progress'},
    'returned': {'type': 'R. returned', 'state': 'in progress'},
    'returned_finish': {'type': 'R. returned', 'state': 'final'},
    'performer_not_found': {'type': 'X. cancelled', 'state': 'final'},
    'return_arrived': {'type': '3. pickuped', 'state': 'in progress'},
    'delivered_finish': {'type': '4. delivered', 'state': 'final'},
    'failed': {'type': 'X. cancelled', 'state': 'final'},
    'accepted': {'type': '1. created', 'state': 'in progress'},
    'new': {'type': '1. created', 'state': 'in progress'},
    'pickup_arrived': {'type': '2. assigned', 'state': 'in progress'},
    'estimating_failed': {'type': 'X. cancelled', 'state': 'final'},
    'cancelled_with_payment': {'type': 'X. cancelled', 'state': 'final'}
}

def calculate_distance(row):
    location_1 = (row["lat"], row["lon"])
    location_2 = (row["store_lat"], row["store_lon"])
    row["linear_distance"] = round(hs.haversine(location_1, location_2), 2)
    return row


def get_pod_orders():
    service = discovery.build('sheets', 'v4', discoveryServiceUrl=
    'https://sheets.googleapis.com/$discovery/rest?version=v4',
                              developerKey=SHEET_KEY)

    spreadsheet_id = SHEET_ID
    range_ = 'A:A'

    request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_)
    response = request.execute()
    pod_orders = [item for sublist in response["values"] for item in sublist]
    return pod_orders


def check_for_pod(row, orders_with_pod):
    if row["status"] not in ["delivered", "delivered_finish"]:
        row["proof"] = "-"
        return row
    if str(row["client_id"]) in orders_with_pod:
        row["proof"] = "Proof provided"
    else:
        row["proof"] = "No proof"
    return row


def get_cod_orders():
    service = discovery.build('sheets', 'v4', discoveryServiceUrl=
    'https://sheets.googleapis.com/$discovery/rest?version=v4',
                              developerKey=COD_SHEET_KEY)
    spreadsheet_id = COD_SHEET_ID

    range_ = 'C:C'
    request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_)
    response = request.execute()
    cod_orders = [item for sublist in response["values"] for item in sublist]
    cod_orders = [item.replace(' ', '').replace('TRK', '') for item in cod_orders]

    range_ = 'E:E'
    request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_)
    response = request.execute()
    cod_links = [item for sublist in response["values"] for item in sublist]

    orders_with_links = dict(zip(cod_orders, cod_links))
    return orders_with_links


def check_for_cod(row, orders_with_cod: dict):
    if row["price_of_goods"] < 1:
        row["cash_collected"] = "Prepaid"
        row["cash_prooflink"] = "Prepaid"
        return row
    if row["status"] not in ["delivered", "delivered_finish"]:
        row["cash_collected"] = "-"
        row["cash_prooflink"] = "-"
        return row
    if str(row["client_id"]) in orders_with_cod.keys():
        row["cash_collected"] = "Deposit verified"
        row["cash_prooflink"] = orders_with_cod[row["client_id"]]
    else:
        row["cash_collected"] = "Not verified"
        row["cash_prooflink"] = "No link"
    return row

  
def check_for_lateness(row):
    if option == "Today":
        cutoff_time = datetime.datetime.strptime(f"{datetime.datetime.today().strftime('%Y-%m-%d')} {row['cutoff']}", "%Y-%m-%d %H:%M")
        current_time = datetime.datetime.now().astimezone(timezone(client_timezone)).replace(tzinfo=None)
        if cutoff_time > current_time:
            difference_munutes = 0  # ignore such cases
        else:
            difference = current_time - cutoff_time
            difference_munutes = int(difference.total_seconds()) / 60
    elif option == "Yesterday":
        difference_munutes = 999  # magic number that is >30
    try:
        created_amt = row["1. created"]
    except:
        created_amt = "-"
    try:
        assigned_amt = row["2. assigned"]
    except:
        assigned_amt = "-"
    try:
        pickuped_amt = row["3. pickuped"]
    except:
        pickuped_amt = "-"
    try:
        if (assigned_amt not in ["-", 1] or pickuped_amt not in ["-", 1]) and option == "Yesterday":
            row["cutoff"] = row["cutoff"] + " 🙀🙀🙀"
        elif (created_amt not in ["-", 1] or assigned_amt not in ["-", 1]) and option == "Today" and difference_munutes >= 30:
            row["cutoff"] = row["cutoff"] + " 🙀🙀🙀"
        elif (created_amt not in ["-", 1] or assigned_amt not in ["-", 1]) and option == "Today" and difference_munutes >= 10:
            row["cutoff"] = row["cutoff"] + " 🙀"
    except:
        print("No warnings")
    return row
    
    
def get_claims(date_from, date_to, cursor=0):
    url = API_URL

    timezone_offset = "+03:00" if SECRETS_MAP[selected_client] in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] else "-06:00"
    payload = json.dumps({
        "created_from": f"{date_from}T00:00:00{timezone_offset}",
        "created_to": f"{date_to}T23:59:59{timezone_offset}",
        "limit": 1000,
        "cursor": cursor
    }) if cursor == 0 else json.dumps({"cursor": cursor})

    client_secret = CLAIM_SECRETS[SECRETS_MAP[selected_client]]

    headers = {
        'Content-Type': 'application/json',
        'Accept-Language': 'en',
        'Authorization': f"Bearer {client_secret}"
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    claims = json.loads(response.text)
    cursor = None
    try:
        cursor = claims['cursor']
        print(f"CURSOR: {cursor}")
    except:
        print("LAST PAGE PROCESSED")
    return claims['claims'], cursor


def get_report(option="Today", start_=None, end_=None) -> pandas.DataFrame:
    offset_back = 0
    if option == "Yesterday":
        offset_back = 1
    elif option == "Tomorrow":
        offset_back = -1

    client_timezone = "Europe/Istanbul" if SECRETS_MAP[selected_client] in [5, 6, 7, 8, 9, 10, 11, 12,
                                                                            13, 14, 15] else "America/Mexico_City"

    if not start_:
        today = datetime.datetime.now(timezone(client_timezone)) - datetime.timedelta(days=offset_back)
        search_from = today.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=3)
        search_to = today.replace(hour=23, minute=59, second=59, microsecond=999999)
        date_from = search_from.strftime("%Y-%m-%d")
        date_to = search_to.strftime("%Y-%m-%d")
    else:
        today = datetime.datetime.now(timezone(client_timezone))
        date_from_offset = datetime.datetime.fromisoformat(start_).astimezone(
            timezone(client_timezone)) - datetime.timedelta(days=2)
        date_from = date_from_offset.strftime("%Y-%m-%d")
        date_to = end_

    today = today.strftime("%Y-%m-%d")
    report = []
    claims, cursor = get_claims(date_from, date_to)
    while cursor:
        new_page_claims, cursor = get_claims(date_from, date_to, cursor)
        claims = claims + new_page_claims
    for claim in claims:
        try:
            claim_from_time = claim['same_day_data']['delivery_interval']['from']
        except:
            continue
        cutoff_time = datetime.datetime.fromisoformat(claim_from_time).astimezone(timezone(client_timezone))
        cutoff_date = cutoff_time.strftime("%Y-%m-%d")
        if not start_:
            if cutoff_date != today:
                continue
        report_cutoff = cutoff_time.strftime("%Y-%m-%d %H:%M")
        report_client_id = claim['route_points'][1]['external_order_id']
        report_claim_id = claim['id']
        report_pickup_address = claim['route_points'][0]['address']['fullname']
        report_pod_point_id = str(claim['route_points'][1]['id'])
        report_receiver_address = claim['route_points'][1]['address']['fullname']
        report_receiver_phone = claim['route_points'][1]['contact']['phone']
        report_receiver_name = claim['route_points'][1]['contact']['name']
        report_status = claim['status']
        report_status_time = claim['updated_ts']
        report_store_name = claim['route_points'][0]['contact']['name']
        report_longitude = claim['route_points'][1]['address']['coordinates'][0]
        report_latitude = claim['route_points'][1]['address']['coordinates'][1]
        report_store_longitude = claim['route_points'][0]['address']['coordinates'][0]
        report_store_latitude = claim['route_points'][0]['address']['coordinates'][1]
        try: 
            report_status_type = statuses[report_status]['type']
            report_status_is_final = statuses[report_status]['state']
        except:
            report_status_type = "?. other"
            report_status_is_final = "unknown"
        try:
            report_courier_name = claim['performer_info']['courier_name']
            report_courier_park = claim['performer_info']['legal_name']
        except:
            report_courier_name = "No courier yet"
            report_courier_park = "No courier yet"
        try:
            report_return_reason = str(claim['route_points'][1]['return_reasons'])
            report_return_comment = str(claim['route_points'][1]['return_comment'])
        except:
            report_return_reason = "No return reasons"
            report_return_comment = "No return comments"
        try:
            report_autocancel_reason = claim['autocancel_reason']
        except:
            report_autocancel_reason = "No cancel reasons"
        try:
            report_route_id = claim['route_id']
        except:
            report_route_id = "No route"
        try:
            report_price_of_goods = 0
            for item in claim['items']:
                report_price_of_goods += float(item['cost_value'])
        except:
            report_price_of_goods = 0
        try:
            report_goods = ""
            for item in claim['items']:
                report_goods = report_goods + str(item['title']) + " |"
        except:
            report_goods = "Not specified"
        try:
            report_weight_kg = 0.0
            for item in claim['items']:
                if re.findall(r"(\d*\.?\d+)\s*(kgs?)\b", str(item['title']), flags=re.IGNORECASE):
                    report_weight_kg = report_weight_kg + float(re.findall(r"(\d*\.?\d+)\s*(kgs?)\b", str(item['title']), flags=re.IGNORECASE)[0][0])
        except:
            report_weight_kg = "Not found"
        row = [report_cutoff, report_client_id, report_claim_id, report_pod_point_id,
               report_pickup_address, report_receiver_address, report_receiver_phone, report_receiver_name,
               report_status, report_status_time, report_store_name, report_courier_name, report_courier_park,
               report_return_reason, report_return_comment, report_autocancel_reason, report_route_id,
               report_longitude, report_latitude, report_store_longitude, report_store_latitude, report_price_of_goods, report_goods, 
               report_weight_kg, report_status_type, report_status_is_final]
        report.append(row)

    result_frame = pandas.DataFrame(report,
                                    columns=["cutoff", "client_id", "claim_id", "pod_point_id",
                                             "pickup_address", "receiver_address", "receiver_phone",
                                             "receiver_name", "status", "status_time",
                                             "store_name", "courier_name", "courier_park",
                                             "return_reason", "return_comment", "cancel_comment",
                                             "route_id", "lon", "lat", "store_lon", "store_lat", "price_of_goods", "items",
                                             "extracted_weight", "type", "is_final"])
    orders_with_pod = get_pod_orders()
    result_frame = result_frame.apply(lambda row: calculate_distance(row), axis=1)
    result_frame = result_frame.apply(lambda row: check_for_pod(row, orders_with_pod), axis=1)
    orders_with_cod = get_cod_orders()
    if option != "Tomorrow":
        try:
            result_frame.insert(3, 'proof', result_frame.pop('proof'))
        except:
            print("POD malfunction, skip column reorder")
#     if selected_client in ["Not specified"]:
#         result_frame = result_frame.apply(lambda row: check_for_cod(row, orders_with_cod), axis=1)
#         result_frame.insert(4, 'cash_collected', result_frame.pop('cash_collected'))
#         result_frame.insert(5, 'cash_prooflink', result_frame.pop('cash_prooflink'))
#         result_frame.insert(6, 'price_of_goods', result_frame.pop('price_of_goods'))
    return result_frame


streamlit_analytics.start_tracking()
st.markdown(f"# Routes report")

if st.sidebar.button("Refresh data", type="primary"):
    st.cache_data.clear()
st.sidebar.caption(f"Page reload doesn't refresh the data.\nInstead, use this button to get a fresh report")

selected_client = st.sidebar.selectbox(
    "Select client:",
    ["Petco", "Pets Table", "Huevos", "Inkovsky", "Baby Creisy", "Vigilancia Network", "Lens Market", "Ebebek", "Supplementer", "Sadece-eczane", "Osevio Internet Hizmetleri",
     "Mevsimi", "Candy Gift", "Akel", "Espresso Perfetto", "Ceviz Agaci", "Guven Sanat"]
)

if selected_client == "Petco":
    st.caption("Petco POD % metric now includes photos uploaded in the app. Data is synchronized every hour (once every XX:00)")

option = st.sidebar.selectbox(
    "Select report date:",
    ["Today", "Yesterday", "Tomorrow", "Monthly", "April"]
)


@st.cache_data
def get_cached_report(period):

    if option == "Monthly":
        report = get_report(period, start_="2023-05-01", end_="2023-05-31")
    elif option == "April":
        report = get_report(period, start_="2023-04-01", end_="2023-04-30")
    else:
        report = get_report(period)
    df_rnt = report[~report['status'].isin(["cancelled", "performer_not_found", "failed"])]
    df_rnt = df_rnt.groupby(['courier_name', 'route_id', 'store_name'])['pickup_address'].nunique().reset_index()
    routes_not_taken = df_rnt[(df_rnt['courier_name'] == "No courier yet") & (df_rnt['route_id'] != "No route")]
    del df_rnt
    try:
        pod_provision_rate = len(report[report['proof'] == "Proof provided"]) / len(
            report[report['status'].isin(['delivered', 'delivered_finish'])])
        pod_provision_rate = f"{pod_provision_rate:.0%}"
    except:
        pod_provision_rate = "--"
    delivered_today = len(report[report['status'].isin(['delivered', 'delivered_finish'])])
    return report, routes_not_taken, pod_provision_rate, delivered_today


df, routes_not_taken, pod_provision_rate, delivered_today = get_cached_report(option)

statuses = st.sidebar.multiselect(
    'Filter by status:',
    ['delivered',
     'pickuped',
     'returning',
     'cancelled_by_taxi',
     'delivery_arrived',
     'cancelled',
     'performer_lookup',
     'performer_found',
     'performer_draft',
     'returned',
     'returned_finish',
     'performer_not_found',
     'return_arrived',
     'delivered_finish',
     'failed',
     'accepted',
     'new',
     'pickup_arrived'])

stores = st.sidebar.multiselect(
    "Filter by stores:",
    df["store_name"].unique()
)

couriers = st.sidebar.multiselect(
    "Filter by courier:",
    df["courier_name"].unique()
)

only_no_proofs = st.sidebar.checkbox("Only parcels without proofs")

if only_no_proofs:
    df = df[df["proof"] == "No proof"]

without_cancelled = st.sidebar.checkbox("Without cancels")

if without_cancelled:
    df = df[~df["status"].isin(["cancelled", "performer_not_found", "failed", "estimating_failed", "cancelled_by_taxi", "cancelled_with_payment"])]    
    
col1, col2, col3 = st.columns(3)
col1.metric("Not pickuped routes :minibus:", str(len(routes_not_taken)))
if pod_provision_rate == "100%": 
  col2.metric("POD provision :100:", pod_provision_rate)
else:
  col2.metric("POD provision :camera:", pod_provision_rate)
col3.metric(f"Delivered {option.lower()} :package:", delivered_today)

if (not statuses or statuses == []) and (not stores or stores == []):
    filtered_frame = df
elif statuses and not stores:
    filtered_frame = df[df['status'].isin(statuses)]
elif stores and not statuses:
    filtered_frame = df[df['store_name'].isin(stores)]
else:
    filtered_frame = df[(df['store_name'].isin(stores)) & (df['store_name'].isin(statuses))]

if couriers:
    filtered_frame = df[df['courier_name'].isin(couriers)]

st.dataframe(filtered_frame)

client_timezone = "Europe/Istanbul" if SECRETS_MAP[selected_client] in [6, 7, 8, 9, 10, 11, 12,
                                                                        13, 14, 15, 16] else "America/Mexico_City"
TODAY = datetime.datetime.now(timezone(client_timezone)).strftime("%Y-%m-%d") \
    if option == "Today" \
    else datetime.datetime.now(timezone(client_timezone)) - datetime.timedelta(days=1)

stores_with_not_taken_routes = ', '.join(str(x) for x in routes_not_taken["store_name"].unique())
st.caption(
    f'Total of :blue[{len(filtered_frame)}] orders in the table. Following stores have not pickuped routes: :red[{stores_with_not_taken_routes}]')

with pandas.ExcelWriter(FILE_BUFFER, engine='xlsxwriter') as writer:
    df.to_excel(writer, sheet_name='routes_report')
    writer.close()

    st.download_button(
        label="Download report as xlsx",
        data=FILE_BUFFER,
        file_name=f"route_report_{TODAY}.xlsx",
        mime="application/vnd.ms-excel"
    )

with st.expander(":round_pushpin: Orders on a map"):
    st.caption(
        f'Hover order to see details. Stores are the big points on a map. :green[Green] orders are delivered, and :red[red] – are the in delivery state. :orange[Orange] are returned or returning. Gray are cancelled.')
    chart_data_delivered = filtered_frame[filtered_frame["status"].isin(['delivered', 'delivered_finish'])]
    chart_data_in_delivery = filtered_frame[~filtered_frame["status"].isin(
        ['delivered', 'delivered_finish', 'cancelled', 'cancelled_by_taxi', 'returning', 'returned_finish',
         'return_arrived'])]
    chart_data_returns = filtered_frame[
        filtered_frame["status"].isin(['returning', 'returned_finish', 'return_arrived'])]
    chart_data_cancels = filtered_frame[filtered_frame["status"].isin(['cancelled', 'cancelled_by_taxi'])]
    view_state_lat = filtered_frame['lat'].iloc[0]
    view_state_lon = filtered_frame['lon'].iloc[0]
    filtered_frame['cutoff'] = filtered_frame['cutoff'].str.split(' ').str[1]
    stores_on_a_map = filtered_frame.groupby(['store_name', 'store_lon', 'store_lat'])['cutoff'].agg(
        lambda x: ', '.join(x.unique())).reset_index(drop=False)
    stores_on_a_map.columns = ['store_name', 'store_lon', 'store_lat', 'cutoff']
    st.pydeck_chart(pdk.Deck(
        map_style=None,
        height=1200,
        initial_view_state=pdk.ViewState(
            latitude=view_state_lat,
            longitude=view_state_lon,
            zoom=10,
            pitch=0,
        ),
        tooltip={"text": "{store_name} : {cutoff}\n{courier_name} : {status}\n{client_id} : {claim_id}"},
        layers=[
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_delivered,
                get_position='[lon, lat]',
                get_color='[11, 102, 35, 160]',
                get_radius=200,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_in_delivery,
                get_position='[lon, lat]',
                get_color='[200, 30, 0, 160]',
                get_radius=200,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_cancels,
                get_position='[lon, lat]',
                get_color='[215, 210, 203, 200]',
                get_radius=200,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_returns,
                get_position='[lon, lat]',
                get_color='[237, 139, 0, 160]',
                get_radius=200,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=filtered_frame,
                get_position='[store_lon, store_lat]',
                get_color='[0, 128, 255, 160]',
                get_radius=250,
                pickable=True
            ),
            pdk.Layer(
                'TextLayer',
                data=stores_on_a_map,
                get_position='[store_lon, store_lat]',
                get_text='store_name',
                get_color='[0, 128, 255]',
                get_size=14,
                get_pixel_offset='[0, 20]',
                pickable=False
            ),
            pdk.Layer(
                'TextLayer',
                data=stores_on_a_map,
                get_position='[store_lon, store_lat]',
                get_text='cutoff',
                get_color='[0, 128, 255]',
                get_size=14,
                get_pixel_offset='[0, 40]',
                pickable=False
            )
        ],
    ))

# if selected_client == "Quiken":
#     with st.expander(":moneybag: Unreported cash on couriers:"):
#         st.caption(f'Shows, how much money couriers have with them – and for how many orders. Counting only delivered orders without proof of deposit provided.')
#         cash_management_df = df[(df["status"].isin(['delivered', 'delivered_finish'])) & (df["cash_collected"] == "Not verified")]
#         st.dataframe(cash_management_df.groupby(['courier_name'])['price_of_goods'].agg(['sum', 'count']).reset_index())

with st.expander(":clipboard: Store/ route details"): 
    pivot_report_frame = pandas.pivot_table(filtered_frame, values='claim_id', index=['store_name', 'cutoff', 'courier_name'], columns=['type'], aggfunc=lambda x: len(x.unique()), fill_value="-").reset_index()
    pivot_report_frame = pivot_report_frame.apply(lambda row: check_for_lateness(row), axis=1)
    only_cats = st.checkbox("Only concerned routes")
    if only_cats:
        pivot_report_frame = pivot_report_frame[pivot_report_frame['cutoff'].str.contains('🙀')]
    st.dataframe(pivot_report_frame, use_container_width=True)

streamlit_analytics.stop_tracking()
