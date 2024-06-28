import streamlit as st
import clickhouse_connect
import pandas as pd
from pygwalker.api.streamlit import StreamlitRenderer
import pygwalker as pyg
from scipy.stats import skew, kurtosis
import streamlit.components.v1 as components


# ClickHouse connection details
ch_host = 'a84a1hn9ig.ap-south-1.aws.clickhouse.cloud'
ch_user = 'default'
ch_password = 'dKd.Y9kFMv06x'
ch_database = 'test_db'

# Set Streamlit page configuration
st.set_page_config(
    page_title="Bosch Pack Cycling Data Dashboard",
    layout="wide"
)

# Add Title
st.title("Bosch Pack Cycling Data Dashboard")

# Function to create a new ClickHouse client
def create_client():
    return clickhouse_connect.get_client(
        host=ch_host,
        port=8443,
        username=ch_user,
        password=ch_password,
        database=ch_database,
        secure=True
    )

# Function to connect to ClickHouse and fetch data
@st.cache_data
def fetch_data(query):
    client = create_client()
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    return df

# Define queries to fetch data from the three tables
queries = {
    "Pack Cycling Step Data": "SELECT *, step_date as date FROM test_db.pack_cycling_step_data",
    "Pack Cycling Record Data": "SELECT * FROM test_db.pack_cycling_record_data",
    "Pack Cycling BMS Data": "SELECT * FROM test_db.pack_cycling_bms_data_new"
}

# Fetch data from all tables initially
data_frames = {name: fetch_data(query) for name, query in queries.items()}

# Function to get unique values for filters based on the current filtered DataFrame
def get_unique_values(df, column):
    if column in df.columns:
        return sorted(df[column].dropna().unique())
    return []

# Sidebar filters
st.sidebar.header("Filters")

# Initialize session state for filters
if 'filters' not in st.session_state:
    st.session_state['filters'] = {
        'cycle_index': [],
        'step_type': [],
        'step_number': [],
        'pack_id': [],
        'date': []
    }

# Function to apply filters
def apply_filters(df, filters):
    if filters['cycle_index']:
        df = df[df['cycle_index'].isin(filters['cycle_index'])]
    if filters['step_type']:
        df = df[df['step_type'].isin(filters['step_type'])]
    if 'step_number' in df.columns and filters['step_number']:
        df = df[df['step_number'].isin(filters['step_number'])]
    if filters['pack_id']:
        df = df[df['pack_id'].isin(filters['pack_id'])]
    if filters['date']:
        df = df[df['date'].isin(filters['date'])]
    return df

# Apply current filters to the DataFrame
filtered_data_frames = {name: apply_filters(df, st.session_state['filters']) for name, df in data_frames.items()}

# Update filter options based on the filtered DataFrame
unique_cycle_index = get_unique_values(pd.concat(filtered_data_frames.values()), 'cycle_index')
unique_step_type = get_unique_values(pd.concat(filtered_data_frames.values()), 'step_type')
unique_step_number = get_unique_values(pd.concat(filtered_data_frames.values()), 'step_number')
unique_pack_id = get_unique_values(pd.concat(filtered_data_frames.values()), 'pack_id')
unique_date = get_unique_values(pd.concat(filtered_data_frames.values()), 'date')

# Multiselect filters
st.session_state['filters']['cycle_index'] = st.sidebar.multiselect('Select cycle index', options=unique_cycle_index, default=st.session_state['filters']['cycle_index'])
st.session_state['filters']['step_type'] = st.sidebar.multiselect('Select step type', options=unique_step_type, default=st.session_state['filters']['step_type'])
st.session_state['filters']['step_number'] = st.sidebar.multiselect('Select step number', options=unique_step_number, default=st.session_state['filters']['step_number'])
st.session_state['filters']['pack_id'] = st.sidebar.multiselect('Select pack id', options=unique_pack_id, default=st.session_state['filters']['pack_id'])
st.session_state['filters']['date'] = st.sidebar.multiselect('Select date', options=unique_date, default=st.session_state['filters']['date'])

# Reapply filters to update the filtered DataFrames
filtered_data_frames = {name: apply_filters(df, st.session_state['filters']) for name, df in data_frames.items()}

# Function to find end_voltage_bms
def find_end_voltage_bms(row, df_bms_data):
    match_condition = (
        (df_bms_data['pack_id'] == row['pack_id']) &
        (df_bms_data['date'] == row['date']) &
        (df_bms_data['oneset_date'] == row['oneset_date']) &
        (df_bms_data['step_type'] == row['step_type']) &
        (df_bms_data['step_number'] == row['step_number'])
    )
    matching_rows = df_bms_data[match_condition]
    if not matching_rows.empty:
        return matching_rows['sumvoltage'].min()
    else:
        return 0

# Add end_voltage_bms to Pack Cycling Step Data
data_frames['Pack Cycling Step Data']['end_voltage_bms'] = data_frames['Pack Cycling Step Data'].apply(find_end_voltage_bms, axis=1, args=(data_frames['Pack Cycling BMS Data'],))

# Convert end_voltage_bms to float32
data_frames['Pack Cycling Step Data']['end_voltage_bms'] = data_frames['Pack Cycling Step Data']['end_voltage_bms'].astype('float32')


# Calculate computed fields if the required columns are present
def add_computed_fields(df):
    cell_voltage_columns = [f'cellv_{i}' for i in range(1, 22)]
    if all(col in df.columns for col in cell_voltage_columns + ['maxv', 'minv']):
        # Calculate delta_voltage
        df['delta_mv'] = (df['maxv'] - df['minv'])*1000

        # Binning minv, maxv, and delta_mv
        minv_bins = [0, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 3]  # Example bin edges for minv
        maxv_bins = [0, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 3]  # Example bin edges for maxv
        delta_mv_bins = [0, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 300, 400, 1000]  # Example bin edges for delta_mv

        # Create binned columns
        df['minv_bins'] = pd.cut(df['minv'], bins=minv_bins, labels=[f'{minv_bins[i]}-{minv_bins[i+1]}' for i in range(len(minv_bins)-1)],right=False)
        df['maxv_bins'] = pd.cut(df['maxv'], bins=maxv_bins, labels=[f'{maxv_bins[i]}-{maxv_bins[i+1]}' for i in range(len(maxv_bins)-1)],right=False)
        df['delta_mv_bins'] = pd.cut(df['delta_mv'], bins=delta_mv_bins, labels=[f'{delta_mv_bins[i]}-{delta_mv_bins[i+1]}' for i in range(len(delta_mv_bins)-1)],right=False)
        
        
        # Calculate median_cell_voltage
        df['median_cell_voltage'] = df[cell_voltage_columns].median(axis=1)

        # Calculate average_cell_voltage
        df['average_cell_voltage'] = df[cell_voltage_columns].mean(axis=1)

        # Calculate 25th percentile cell voltage
        df['25th_p_cellv'] = df[cell_voltage_columns].quantile(0.25, axis=1)

        # Calculate 75th percentile cell voltage
        df['75th_p_cellv'] = df[cell_voltage_columns].quantile(0.75, axis=1)

        # Calculate standard deviation of cell voltage
        df['stdev_cell_v'] = df[cell_voltage_columns].std(axis=1)

        # Calculate minimum cell voltage
        df['min_cell_voltage'] = df[cell_voltage_columns].min(axis=1)

        # Calculate maximum cell voltage
        df['max_cell_voltage'] = df[cell_voltage_columns].max(axis=1)

        # Calculate range of cell voltage
        df['range_cell_voltage'] = df['max_cell_voltage'] - df['min_cell_voltage']

        # Calculate variance of cell voltage
        df['variance_cell_voltage'] = df[cell_voltage_columns].var(axis=1)

        # Calculate coefficient of variation
        df['coefficient_of_variation'] = (df['stdev_cell_v'] / df['average_cell_voltage']) * 100

        # Calculate skewness of cell voltage
        df['skewness'] = df[cell_voltage_columns].apply(lambda row: skew(row), axis=1)

        # Calculate kurtosis of cell voltage
        df['kurtosis'] = df[cell_voltage_columns].apply(lambda row: kurtosis(row), axis=1)

        # Calculate interquartile range (IQR) of cell voltage
        df['iqr_cell_voltage'] = df['75th_p_cellv'] - df['25th_p_cellv']
    return df

# Add computed fields to the filtered data frames
filtered_data_frames = {name: add_computed_fields(df) for name, df in filtered_data_frames.items()}

# Streamlit tabs for different dataframes
tab1, tab2, tab3 = st.tabs(["Step Data", "Record Data", "BMS Data"])

with tab1:
    st.header("Pack Cycling Step Data")
    st.dataframe(filtered_data_frames["Pack Cycling Step Data"])
    vis_spec = r"""{"config":[{"config":{"defaultAggregated":true,"geoms":["table"],"coordSystem":"generic","limit":-1,"timezoneDisplayOffset":0},"encodings":{"dimensions":[{"fid":"primary_id","name":"primary_id","basename":"primary_id","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"pack_id","name":"pack_id","basename":"pack_id","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"step_number","name":"step_number","basename":"step_number","analyticType":"dimension","semanticType":"nominal","aggName":"sum","offset":0},{"fid":"step_date","name":"step_date","basename":"step_date","semanticType":"temporal","analyticType":"dimension","offset":0},{"fid":"cycle_index","name":"cycle_index","basename":"cycle_index","semanticType":"quantitative","analyticType":"dimension","offset":0},{"fid":"step_index","name":"step_index","basename":"step_index","semanticType":"quantitative","analyticType":"dimension","offset":0},{"fid":"step_type","name":"step_type","basename":"step_type","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"step_time","name":"step_time","basename":"step_time","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"oneset_date","name":"oneset_date","basename":"oneset_date","semanticType":"temporal","analyticType":"dimension","offset":0},{"fid":"end_date","name":"end_date","basename":"end_date","semanticType":"temporal","analyticType":"dimension","offset":0},{"fid":"date","name":"date","basename":"date","semanticType":"temporal","analyticType":"dimension","offset":0},{"fid":"gw_mea_key_fid","name":"Measure names","analyticType":"dimension","semanticType":"nominal"}],"measures":[{"fid":"capacity_ah","name":"capacity_ah","basename":"capacity_ah","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"spec_cap_mah_g","name":"spec_cap_mah_g","basename":"spec_cap_mah_g","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"chg_cap_ah","name":"chg_cap_ah","basename":"chg_cap_ah","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"chg_spec_cap_mah_g","name":"chg_spec_cap_mah_g","basename":"chg_spec_cap_mah_g","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"dchg_cap_ah","name":"dchg_cap_ah","basename":"dchg_cap_ah","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"dchg_spec_cap_mah_g","name":"dchg_spec_cap_mah_g","basename":"dchg_spec_cap_mah_g","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"net_dchg_cap_ah","name":"net_dchg_cap_ah","basename":"net_dchg_cap_ah","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"energy_wh","name":"energy_wh","basename":"energy_wh","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"spec_energy_mwh_g","name":"spec_energy_mwh_g","basename":"spec_energy_mwh_g","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"chg_energy_wh","name":"chg_energy_wh","basename":"chg_energy_wh","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"chg_spec_energy_mwh_g","name":"chg_spec_energy_mwh_g","basename":"chg_spec_energy_mwh_g","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"dchg_energy_wh","name":"dchg_energy_wh","basename":"dchg_energy_wh","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"dchg_spec_energy_mwh_g","name":"dchg_spec_energy_mwh_g","basename":"dchg_spec_energy_mwh_g","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"net_dchg_energy_wh","name":"net_dchg_energy_wh","basename":"net_dchg_energy_wh","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"super_capacitor_f","name":"super_capacitor_f","basename":"super_capacitor_f","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"oneset_volt_v","name":"oneset_volt_v","basename":"oneset_volt_v","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"chg_oneset_volt_v","name":"chg_oneset_volt_v","basename":"chg_oneset_volt_v","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"dchg_oneset_volt_v","name":"dchg_oneset_volt_v","basename":"dchg_oneset_volt_v","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"end_voltage_v","name":"end_voltage_v","basename":"end_voltage_v","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"end_of_chgvolt_v","name":"end_of_chgvolt_v","basename":"end_of_chgvolt_v","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"end_of_dchgvolt_v","name":"end_of_dchgvolt_v","basename":"end_of_dchgvolt_v","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"chg_med_volt_v","name":"chg_med_volt_v","basename":"chg_med_volt_v","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"dchg_med_volt_v","name":"dchg_med_volt_v","basename":"dchg_med_volt_v","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"starting_current_a","name":"starting_current_a","basename":"starting_current_a","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"end_current_a","name":"end_current_a","basename":"end_current_a","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"dcir_m_ohm","name":"dcir_m_ohm","basename":"dcir_m_ohm","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"end_voltage_bms","name":"end_voltage_bms","basename":"end_voltage_bms","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"gw_count_fid","name":"Row count","analyticType":"measure","semanticType":"quantitative","aggName":"sum","computed":true,"expression":{"op":"one","params":[],"as":"gw_count_fid"}},{"fid":"gw_mea_val_fid","name":"Measure values","analyticType":"measure","semanticType":"quantitative","aggName":"sum"}],"rows":[{"fid":"oneset_date","name":"oneset_date","basename":"oneset_date","semanticType":"temporal","analyticType":"dimension","offset":0},{"fid":"step_number","name":"step_number","basename":"step_number","analyticType":"dimension","semanticType":"nominal","aggName":"sum","offset":0}],"columns":[{"fid":"pack_id","name":"pack_id","basename":"pack_id","semanticType":"nominal","analyticType":"dimension","offset":0},{"fid":"capacity_ah","name":"capacity_ah","basename":"capacity_ah","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"oneset_volt_v","name":"oneset_volt_v","basename":"oneset_volt_v","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0},{"fid":"end_voltage_bms","name":"end_voltage_bms","basename":"end_voltage_bms","analyticType":"measure","semanticType":"quantitative","aggName":"sum","offset":0}],"color":[],"opacity":[],"size":[],"shape":[],"radius":[],"theta":[],"longitude":[],"latitude":[],"geoId":[],"details":[],"filters":[{"fid":"pack_id","name":"pack_id","basename":"pack_id","semanticType":"nominal","analyticType":"dimension","offset":0,"rule":{"type":"one of","value":["LB05A22J000086"]}},{"fid":"step_type","name":"step_type","basename":"step_type","semanticType":"nominal","analyticType":"dimension","offset":0,"rule":{"type":"one of","value":["CC DChg"]}},{"fid":"step_number","name":"step_number","basename":"step_number","analyticType":"dimension","semanticType":"nominal","aggName":"sum","offset":0,"rule":{"type":"not in","value":[]}}],"text":[]},"layout":{"showActions":false,"showTableSummary":false,"stack":"stack","interactiveScale":false,"zeroScale":true,"size":{"mode":"auto","width":320,"height":200},"format":{},"geoKey":"name","resolve":{"x":false,"y":false,"color":false,"opacity":false,"shape":false,"size":false}},"visId":"gw_D4Sx","name":"Capacity Table"}],"chart_map":{},"workflow_list":[{"workflow":[{"type":"filter","filters":[{"fid":"pack_id","rule":{"type":"one of","value":["LB05A22J000086"]}},{"fid":"step_type","rule":{"type":"one of","value":["CC DChg"]}},{"fid":"step_number","rule":{"type":"not in","value":[]}}]},{"type":"view","query":[{"op":"aggregate","groupBy":["pack_id","oneset_date","step_number"],"measures":[{"field":"capacity_ah","agg":"sum","asFieldKey":"capacity_ah_sum"},{"field":"oneset_volt_v","agg":"sum","asFieldKey":"oneset_volt_v_sum"},{"field":"end_voltage_bms","agg":"sum","asFieldKey":"end_voltage_bms_sum"}]}]}]}],"version":"0.4.8.9"}"""

    walker = pyg.walk(filtered_data_frames["Pack Cycling Step Data"], env='Streamlit', spec=vis_spec)
    components.html(walker.to_html(), height=800)

with tab2:
    st.header("Pack Cycling Record Data")
    st.dataframe(filtered_data_frames["Pack Cycling Record Data"])
    # Use PyG Walker for data exploration
    walker = pyg.walk(filtered_data_frames["Pack Cycling Record Data"], env='Streamlit', spec=vis_spec)
    components.html(walker.to_html(), height=800)


with tab3:
    st.header("Pack Cycling BMS Data")
    st.dataframe(filtered_data_frames["Pack Cycling BMS Data"])
   
    walker = pyg.walk(filtered_data_frames["Pack Cycling BMS Data"], env='Streamlit')
    components.html(walker.to_html(), height=800)
