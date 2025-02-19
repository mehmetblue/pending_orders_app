# Import required Python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched
import pandas as pd

# Set up the app title and description
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("Orders that need to be filled.")

# Establish a connection to Snowflake
cnx = st.connection("snowflake", type="snowflake")

# Get the active Snowflake session
# session = get_active_session()  # Uncomment this if using a default session
session = cnx.session()  # Use the session from the Snowflake connection

# Retrieve orders from the database (filtering only unfilled orders)
my_dataframe = session.table("smoothies.public.orders") \
                      .select(col("ORDER_UID"), col("INGREDIENTS"), col("NAME_ON_ORDER"), col("ORDER_FILLED")) \
                      .filter(col("ORDER_FILLED") == False) \
                      .collect()

# Convert the retrieved Snowflake data into a Pandas DataFrame
df_orders = pd.DataFrame(my_dataframe)

if not df_orders.empty:
    # Add a sequential order number column for display
    df_orders.insert(0, "Order #", range(1, len(df_orders) + 1))

    # Allow users to update the order status using an editable data table
    editable_df = st.data_editor(df_orders, num_rows="dynamic", key="order_table")

    # Display a submit button for saving changes
    submitted = st.button('Submit')

    if submitted:
        # Convert the updated DataFrame into a Snowflake-compatible format
        edited_dataset = session.create_dataframe(editable_df)

        try:
            # Use the MERGE function to update the ORDER_FILLED column in the database
            og_dataset = session.table("smoothies.public.orders")
            og_dataset.merge(
                edited_dataset,
                (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )
        except:
            # Show a success message after updating the order status
            st.toast("Order status updated successfully! ", icon="✅")
            st.rerun()  # Refresh the page to display updated data

else:
    # Display a message when there are no pending orders
    st.success('There are no pending orders right now', icon="🎉")
