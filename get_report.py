import datetime
import requests
import json
import pandas
from pytz import timezone
from googleapiclient import discovery
import streamlit as st
import streamlit_analytics

CLAIM_SECRET = st.secrets["CLAIM_SECRET"]
SHEET_KEY = st.secrets["SHEET_KEY"]
SHEET_ID = st.secrets["SHEET_ID"]
API_URL = st.secrets["API_URL"]


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


def get_claims(date_from, date_to):
    url = API_URL

    payload = json.dumps({
        "cursor": 0,
        "created_from": f"{date_from}T00:00:00-06:00",
        "created_to": f"{date_to}T23:59:59-06:00",
        "limit": 1000,
    })
    headers = {
        'Content-Type': 'application/json',
        'Accept-Language': 'en',
        'Authorization': f"Bearer {CLAIM_SECRET}"
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    claims = json.loads(response.text)
    return claims['claims']


def get_report(option: str = "Today") -> pandas.DataFrame:
    offset_back = 0
    if option == "Yesterday":
        offset_back = 1
    today = datetime.datetime.now(timezone("America/Mexico_City")) - datetime.timedelta(days=offset_back)
    search_from = today.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=3)
    search_to = today.replace(hour=23, minute=59, second=59, microsecond=999999)

    date_from = search_from.strftime("%Y-%m-%d")
    date_to = search_to.strftime("%Y-%m-%d")
    today = today.strftime("%Y-%m-%d")

    claims = get_claims(date_from, date_to)
    report = []
    for claim in claims:
        claim_from_time = claim['same_day_data']['delivery_interval']['from']
        cutoff_time = datetime.datetime.fromisoformat(claim_from_time).astimezone(timezone("America/Mexico_City"))
        cutoff_date = cutoff_time.strftime("%Y-%m-%d")
        if cutoff_date != today:
            continue
        report_date = today
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
        try:
            report_courier_name = claim['performer_info']['courier_name']
            report_courier_park = claim['performer_info']['legal_name']
        except:
            report_courier_name = "No courier yet"
            report_courier_park = "No courier yet"
        try:
            report_route_id = claim['route_id']
        except:
            report_route_id = "No route"
        row = [report_date, report_cutoff, report_client_id, report_claim_id,
               report_pickup_address, report_receiver_address, report_receiver_phone, report_receiver_name,
               report_status, report_status_time, report_store_name, report_courier_name, report_courier_park, report_route_id]
        report.append(row)

    result_frame = pandas.DataFrame(report,
                                    columns=["date", "cutoff", "client_id", "claim_id",
                                             "pickup_address", "receiver_address", "receiver_phone",
                                             "receiver_name", "status", "status_time",
                                             "store_name", "courier_name", "courier_park", "route_id"])
    orders_with_pod = get_pod_orders()
    result_frame = result_frame.apply(lambda row: check_for_pod(row, orders_with_pod), axis=1)
    return result_frame

streamlit_analytics.start_tracking()
st.markdown(f"# Routes report")

if st.button("Refresh data"):
    st.experimental_memo.clear()

option = st.selectbox(
    "Select report date:",
    ["Today", "Yesterday"]
)

@st.experimental_memo
def get_cached_report(period):
    report = get_report(period)  # This makes the function take 2s to run
    df_rnt = report.groupby(['store_name', 'courier_name'])['route_id'].nunique().reset_index()
    routes_not_taken = str(len(df_rnt[df_rnt['courier_name'] == "No courier yet"]))
    try:
        pod_provision_rate = len(report[report['proof'] == "Proof provided"]) / len(report[report['status'].isin(['delivered', 'delivered_finish'])])
        pod_provision_rate = f"{pod_provision_rate:.0%}"
    except:
        pod_provision_rate = "--"
    delivered_today = len(report[report['status'].isin(['delivered', 'delivered_finish'])])
    return report, routes_not_taken, pod_provision_rate, delivered_today

df, routes_not_taken, pod_provision_rate = get_cached_report(option)

statuses = st.multiselect(
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

col1, col2, col3 = st.columns(3)
col1.metric("Routes not taken", routes_not_taken)
col2.metric("POD provision", pod_provision_rate)
col3.metric("Delivered today", pod_provision_rate)

if not statuses or statuses == []:
    df
else:
    df[df['status'].isin(statuses)]


TODAY = datetime.datetime.now(timezone("America/Mexico_City")).strftime("%Y-%m-%d") \
    if option == "Today" \
    else datetime.datetime.now(timezone("America/Mexico_City")) - datetime.timedelta(days=1)


@st.experimental_memo
def convert_df(dataframe: pandas.DataFrame):
    return dataframe.to_csv().encode('utf-8')
xlsx_report = convert_df(df)


st.download_button(
    label="Download report as csv",
    data=xlsx_report,
    file_name=f'route_report_{TODAY}.csv',
    mime='text/csv',
)
streamlit_analytics.stop_tracking()
