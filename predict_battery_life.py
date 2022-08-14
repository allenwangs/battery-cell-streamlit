import streamlit as st
import pandas as pd
import numpy as np
import streamlit as st
import snowflake.connector
import plotly.figure_factory as ff
import plotly.express as px

st.set_page_config(layout="wide")

# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()


st.title('Data-driven prediction of battery cycle life before capacity degradation')

markdown_text = """
This APP is a reproduction of the plotting in the Nature paper 
[Data-driven prediction of battery cycle life before capacity degradation](https://www.nature.com/articles/s41560-019-0356-8)

For more detail, please refer to this [slide](https://docs.google.com/presentation/d/14_CsxxbkMzV-bq6w0G0rShIcnnrY14s6v-Sr--pQuIM/edit?usp=sharing)

"""
st.markdown(markdown_text)


markdown_text = """
# Data Source
The battery cell testing raw data used in the paper is available at this [website](https://data.matr.io/1/projects/5c48dd2bc625d700019f3204).


Dataset introduction from the website:
> This dataset, used in our publication “Data-driven prediction of battery cycle life before capacity degradation”, consists of 124 commercial lithium-ion batteries cycled to failure under fast-charging conditions. These lithium-ion phosphate (LFP)/graphite cells, manufactured by A123 Systems (APR18650M1A), were cycled in horizontal cylindrical fixtures on a 48-channel Arbin LBT potentiostat in a forced convection temperature chamber set to 30°C. The cells have a nominal capacity of 1.1 Ah and a nominal voltage of 3.3 V.

"""
st.markdown(markdown_text)

st.subheader("Select one file to check raw data")

@st.experimental_memo(ttl=600)
def get_file_list():

    sql = """
    select file_name
    from production.dim_cell_metadata
    where _dbt_source_relation = 'analytics.production.stg_nature_paper_time_series'
    """

    return pd.read_sql(sql, conn)['FILE_NAME'].values

col1, col2 = st.columns(2)

with col1:
    file_name_list = get_file_list()
    file_name = st.selectbox('Select a cycle data', file_name_list)

with col2:
    cycle_index = st.number_input('Select cycle index', value=100)

st.write("Select file:", file_name)
st.write("Select cycle:", cycle_index)

sql = f"""
select
    file_name,
    test_time,
    voltage,
    cell_current,
    cycle_index,
    step_type,
    discharge_capacity,
    charge_capacity
    
from production.fct_time_series
where file_name = '{file_name}'
and cycle_index = {cycle_index}
"""

df_time_series = pd.read_sql(sql, conn)
df_time_series.columns = df_time_series.columns.str.lower()


col3, col4 = st.columns(2)

with col3:
    fig = px.scatter(df_time_series, x="test_time", y="cell_current",
                     color='step_type', color_continuous_scale='RdBu',
                     width=600, height=300,
                     labels={"cycle_index": "Cycle Index",
                             "cell_current": "Current (A)",
                             "cycle_index_end_of_life": "Cycle life"
                             },
                     title="Current")

    st.plotly_chart(fig, use_container_width=False)

with col4:
    fig = px.scatter(df_time_series, x="test_time", y="discharge_capacity",
                     color='step_type', color_continuous_scale='RdBu',
                     width=600, height=300,
                     labels={"cycle_index": "Cycle Index",
                             "discharge_capacity": "Discharge Capacity (Ah)",
                             "cycle_index_end_of_life": "Cycle life"
                             },
                     title="Discharge Capacity")

    st.plotly_chart(fig, use_container_width=False)

col5, col6 = st.columns(2)

with col5:
    fig = px.scatter(df_time_series, x="test_time", y="voltage",
                     color='step_type', color_continuous_scale='RdBu',
                     width=600, height=300,
                     labels={"cycle_index": "Cycle Index",
                             "voltage": "Voltage (V)",
                             "cycle_index_end_of_life": "Cycle life"
                             },
                     title="Voltage")

    st.plotly_chart(fig, use_container_width=False)

with col6:
    fig = px.scatter(df_time_series, x="test_time", y="charge_capacity",
                     color='step_type', color_continuous_scale='RdBu',
                     width=600, height=300,
                     labels={"cycle_index": "Cycle Index",
                             "charge_capacity": "Charge Capacity (Ah)",
                             "cycle_index_end_of_life": "Cycle life"
                             },
                     title="Charge Capacity")

    st.plotly_chart(fig, use_container_width=False)




st.title("Discharge Capacity Over Cycle")


@st.experimental_memo(ttl=600)
def plot_discharge_capacity_over_cycle():


    sql = """
    with get_discharge_capacity_over_cycle as (
      select
          _dbt_source_relation,
          file_name,
          cycle_index,
          max(discharge_capacity) as discharge_capacity
    
      from production.fct_time_series
      where _dbt_source_relation = 'analytics.production.stg_nature_paper_time_series'
      group by 1, 2, 3
      order by 1, 2, 3
    ),
    
    get_end_of_life_info as (
      select
        file_name,
        cycle_index as cycle_index_end_of_life
      from production.dim_cell_metadata
      where _dbt_source_relation = 'analytics.production.stg_nature_paper_time_series'  
    ),
    
    join_end_of_life as (
        select
          get_discharge_capacity_over_cycle._dbt_source_relation,
          get_discharge_capacity_over_cycle.file_name,
          get_discharge_capacity_over_cycle.cycle_index,
          get_discharge_capacity_over_cycle.discharge_capacity,
          get_end_of_life_info.cycle_index_end_of_life
      from get_discharge_capacity_over_cycle
      inner join get_end_of_life_info
        on get_discharge_capacity_over_cycle.file_name = get_end_of_life_info.file_name
    ),
    
    remove_outliers as (
        select *
        from join_end_of_life
        where discharge_capacity < 1.5
        and discharge_capacity > 0.85
    ),
    
    remove_long_life as (
        select *
        from remove_outliers
        where file_name != 'cycler_data/2018-04-12_batch8_CH17.csv'
    )
    
    select * from remove_long_life
    """

    df = pd.read_sql(sql, conn)
    df.columns = df.columns.str.lower()

    fig = px.scatter(df, x="cycle_index", y="discharge_capacity",
                     color='cycle_index_end_of_life', color_continuous_scale='RdBu',
                     width=1000, height=400,
                     labels={
                         "cycle_index": "Cycle Index",
                         "discharge_capacity": "Discharge Capacity (Ah)",
                         "cycle_index_end_of_life": "Cycle life"
                     },
                     title="Discharge Capacity over Cycles")

    # Plot!
    st.plotly_chart(fig, use_container_width=False)


plot_discharge_capacity_over_cycle()


st.title("Change in Voltage Curve")

col7, col8 = st.columns(2)

@st.experimental_memo(ttl=600)
def plot_discharge_capacity_diff_100_10():

    # Fig2
    sql = """
    with random_pick_file as (
        select 
            file_name,
            cycle_index as cycle_index_end_of_life
        from production.dim_cell_metadata 
        where _dbt_source_relation = 'analytics.production.stg_nature_paper_time_series'
    ),
    
    
    cycle_10 as (
        select *
        from production.fct_voltage_series
        where cycle_index = 10
        and file_name in (select file_name from random_pick_file)
    ),
    
    cycle_100 as (
        select *
        from production.fct_voltage_series
        where cycle_index = 100
        and file_name in (select file_name from random_pick_file)
    ),
    
    cycle_difference as (
        select
            cycle_100.file_name,
            cycle_100.voltage,
            cycle_100.discharge_capacity - cycle_10.discharge_capacity as discharge_capacity_diff_100_10
        from cycle_100
        inner join cycle_10
        on cycle_100.file_name = cycle_10.file_name
        and cycle_100.voltage = cycle_10.voltage
    ),
    
    join_life_info as (
        select
            cycle_difference.file_name,
            cycle_difference.voltage,
            cycle_difference.discharge_capacity_diff_100_10,
            random_pick_file.cycle_index_end_of_life
        from cycle_difference
        inner join random_pick_file
            on cycle_difference.file_name = random_pick_file.file_name
    ),
    
    remove_long_life_cell as (
        select *
        from join_life_info
        where file_name != 'cycler_data/2018-04-12_batch8_CH17.csv'
    ),
    
    revmoe_boundary_voltage as (
        select *
        from remove_long_life_cell
        where voltage < 3.25
        and voltage > 2.03
    )
    
    select * from revmoe_boundary_voltage
    
    """

    df_fig2 = pd.read_sql(sql, conn)
    df_fig2.columns = df_fig2.columns.str.lower()

    fig2 = px.scatter(df_fig2, x="discharge_capacity_diff_100_10", y="voltage",
                     color='cycle_index_end_of_life', color_continuous_scale='RdBu',
                     width=600, height=500,
                     labels={
                         "discharge_capacity_diff_100_10": "Q_100 - Q_10 (Ah)",
                         "voltage": "Voltage (V)",
                         "cycle_index_end_of_life": "Cycle life"
                     },
                     title='Difference of the discharge capacity curves')
    with col7:
        st.plotly_chart(fig2, use_container_width=False)


plot_discharge_capacity_diff_100_10()

@st.experimental_memo(ttl=600)
def plot_voltage_curve_variance():

    # Fig4

    sql = """
        with random_pick_file as (
            select 
                file_name,
                cycle_index as cycle_index_end_of_life
            from production.dim_cell_metadata 
            where _dbt_source_relation = 'analytics.production.stg_nature_paper_time_series'
        ),
        
        
        cycle_10 as (
            select *
            from production.fct_voltage_series
            where cycle_index = 10
            and file_name in (select file_name from random_pick_file)
        ),
        
        cycle_100 as (
            select *
            from production.fct_voltage_series
            where cycle_index = 100
            and file_name in (select file_name from random_pick_file)
        ),
        
        cycle_difference as (
            select
                cycle_100.file_name,
                cycle_100.voltage,
                cycle_100.discharge_capacity - cycle_10.discharge_capacity as discharge_capacity_diff_100_10
            from cycle_100
            inner join cycle_10
            on cycle_100.file_name = cycle_10.file_name
            and cycle_100.voltage = cycle_10.voltage
        ),
        
        join_life_info as (
            select
                cycle_difference.file_name,
                cycle_difference.voltage,
                cycle_difference.discharge_capacity_diff_100_10,
                random_pick_file.cycle_index_end_of_life
            from cycle_difference
            inner join random_pick_file
                on cycle_difference.file_name = random_pick_file.file_name
        ),
        
        remove_long_life_cell as (
            select *
            from join_life_info
            where file_name != 'cycler_data/2018-04-12_batch8_CH17.csv'
        ),
        
        revmoe_boundary_voltage as (
            select *
            from remove_long_life_cell
            where voltage < 3.25
            and voltage > 2.03
        ),
        
        calculate_variance as (
            select
                file_name,
                cycle_index_end_of_life,
                variance(discharge_capacity_diff_100_10) as voltage_curve_variance
            from revmoe_boundary_voltage
            group by 1, 2
        )
        
        select * from calculate_variance
    
    """

    df_variance = pd.read_sql(sql, conn)

    df_variance.columns = df_variance.columns.str.lower()

    fig4 = px.scatter(df_variance, x="voltage_curve_variance", y="cycle_index_end_of_life",
                      log_x=True, log_y=True,
                      color='cycle_index_end_of_life', color_continuous_scale='RdBu',
                      width=600, height=500,
                      labels={
                          "voltage_curve_variance": "Var(Q_100 - Q_10(V))",
                          "cycle_index_end_of_life": "Cycle life"
                      },
                      title='Cycle life as a function of voltage curve variance')

    with col8:
        st.plotly_chart(fig4, use_container_width=False)

plot_voltage_curve_variance()