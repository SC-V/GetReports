import datetime
import requests
import json
import pandas
from pytz import timezone
from googleapiclient import discovery
import streamlit as st
import streamlit_analytics
import pydeck as pdk


DEFAULT_CLAIM_SECRET = st.secrets["CLAIM_SECRET"]
CLAIM_SECRETS = st.secrets["CLAIM_SECRETS"]
SHEET_KEY = st.secrets["SHEET_KEY"]
SHEET_ID = st.secrets["SHEET_ID"]
API_URL = st.secrets["API_URL"]
SECRETS_MAP = {"Petco": 0,
               "Quiken": 1,
               "Cubbo": 2,
               "Lens Market": 3}

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


def get_claims(date_from, date_to, cursor=0):
    url = API_URL

    payload = json.dumps({
        "created_from": f"{date_from}T00:00:00-06:00",
        "created_to": f"{date_to}T23:59:59-06:00",
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
    
    client_timezone = "Europe/Istanbul" if CLAIM_SECRETS[SECRETS_MAP[selected_client]] == 3 else "America/Mexico_City"
    
    if not start_:
        today = datetime.datetime.now(timezone(client_timezone)) - datetime.timedelta(days=offset_back)
        search_from = today.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=3)
        search_to = today.replace(hour=23, minute=59, second=59, microsecond=999999)
        date_from = search_from.strftime("%Y-%m-%d")
        date_to = search_to.strftime("%Y-%m-%d")
    else:
        today = datetime.datetime.now(timezone(client_timezone)) - datetime.timedelta(days=offset_back)
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
        if cutoff_date != today and not start_:
            continue
        report_cutoff = cutoff_time.strftime("%Y-%m-%d %H:%M")
        report_client_id = claim['route_points'][1]['external_order_id']
        report_claim_id = claim['id']
        report_pickup_address = claim['route_points'][0]['address']['fullname']
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
            report_courier_name = claim['performer_info']['courier_name']
            report_courier_park = claim['performer_info']['legal_name']
        except:
            report_courier_name = "No courier yet"
            report_courier_park = "No courier yet"
        try:
            report_return_reason = claim['route_points'][1]['return_reasons']
            report_return_comment = claim['route_points'][1]['return_comment']
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
        row = [report_cutoff, report_client_id, report_claim_id,
               report_pickup_address, report_receiver_address, report_receiver_phone, report_receiver_name,
               report_status, report_status_time, report_store_name, report_courier_name, report_courier_park,
               report_return_reason, report_return_comment, report_autocancel_reason, report_route_id,
               report_longitude, report_latitude, report_store_longitude, report_store_latitude]
        report.append(row)

    result_frame = pandas.DataFrame(report,
                                    columns=["cutoff", "client_id", "claim_id",
                                             "pickup_address", "receiver_address", "receiver_phone",
                                             "receiver_name", "status", "status_time",
                                             "store_name", "courier_name", "courier_park",
                                             "return_reason", "return_comment", "cancel_comment",
                                             "route_id", "lon", "lat", "store_lon", "store_lat"])
    orders_with_pod = get_pod_orders()
    result_frame = result_frame.apply(lambda row: check_for_pod(row, orders_with_pod), axis=1)
    result_frame.insert(3, 'proof', result_frame.pop('proof'))
    return result_frame

streamlit_analytics.start_tracking()
st.markdown(f"# Routes report")

if st.sidebar.button("Refresh data", type="primary"):
    st.experimental_memo.clear()
st.sidebar.caption(f"Page reload doesn't refresh the data.\nInstead, use this button to get a fresh report")

selected_client = st.sidebar.selectbox(
    "Select client:",
    ["Petco", "Quiken", "Cubbo", "Lens Market"]
)

option = st.sidebar.selectbox(
    "Select report date:",
    ["Today", "Yesterday"]
)

@st.experimental_memo
def get_cached_report(period):
    report = get_report(period)
    df_rnt = report.groupby(['courier_name', 'route_id', 'store_name'])['pickup_address'].nunique().reset_index()
    routes_not_taken = df_rnt[(df_rnt['courier_name'] == "No courier yet") & (df_rnt['route_id'] != "No route")]
    try:
        pod_provision_rate = len(report[report['proof'] == "Proof provided"]) / len(report[report['status'].isin(['delivered', 'delivered_finish'])])
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
     'performer_found',
     'performer_draft',
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

only_no_proofs = st.sidebar.checkbox("Only parcels without proofs")

if only_no_proofs:
  df = df[df["proof"] == "No proof"]

col1, col2, col3 = st.columns(3)
col1.metric("Not pickuped routes", str(len(routes_not_taken)))
col2.metric("POD provision", pod_provision_rate)
col3.metric(f"Delivered {option.lower()}", delivered_today)

if (not statuses or statuses == []) and (not stores or stores == []):
    filtered_frame = df
elif statuses and not stores:
    filtered_frame = df[df['status'].isin(statuses)]
elif stores and not statuses:
    filtered_frame = df[df['store_name'].isin(stores)]
else:
    filtered_frame = df[(df['store_name'].isin(stores)) & (df['store_name'].isin(statuses))]

st.dataframe(filtered_frame)

client_timezone = "Europe/Istanbul" if CLAIM_SECRETS[SECRETS_MAP[selected_client]] == 3 else "America/Mexico_City"
TODAY = datetime.datetime.now(timezone(client_timezone)).strftime("%Y-%m-%d") \
    if option == "Today" \
    else datetime.datetime.now(timezone(client_timezone)) - datetime.timedelta(days=1)

@st.experimental_memo
def convert_df(dataframe: pandas.DataFrame):
    return dataframe.to_csv().encode('utf-8')

xlsx_report = convert_df(df)

stores_with_not_taken_routes = ', '.join(str(x) for x in routes_not_taken["store_name"].unique())
st.caption(f'Total of :blue[{len(filtered_frame)}] orders in the table. Following stores have not pickuped routes: :red[{stores_with_not_taken_routes}]')
                        
st.download_button(
    label="Download full report as csv",
    data=xlsx_report,
    file_name=f'route_report_{TODAY}.csv',
    mime='text/csv',
)

with st.expander("Orders on a map:"):
    st.caption(f'Hover order to see details. Stores are the big points on a map. :green[Green] orders are delivered, and :red[red] – are the in delivery, returning or cancelled state')
    chart_data_delivered = filtered_frame[filtered_frame["status"].isin(['delivered', 'delivered_finish'])]
    chart_data_in_delivery = filtered_frame[~filtered_frame["status"].isin(['delivered', 'delivered_finish'])]
    view_state_lat = filtered_frame['lat'].iloc[0]
    view_state_lon = filtered_frame['lon'].iloc[0]
    stores_on_a_map = filtered_frame.groupby(['store_name', 'store_lon', 'store_lat'])['cutoff'].apply(list)
    st.pydeck_chart(pdk.Deck(
        map_style=None,
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
                data=filtered_frame,
                get_position='[store_lon, store_lat]',
                get_color='[49, 51, 53, 160]',
                get_radius=250,
                pickable=True
            ),
            pdk.Layer(
                'TextLayer',
                data=stores_on_a_map,
                get_position='[store_lon, store_lat]',
                get_text='[store_name, cutoff]',
                get_color='[0, 0, 0]',
                get_size=12,
                get_pixel_offset='[0, 15]',
                pickable=False
            )
        ],
    ))
    st.image("https://yastatic.net/s3/newbiz-static-yango/v1/0.0.58/assets/img/icons/gif/car-small.gif", width=30)

streamlit_analytics.stop_tracking()
