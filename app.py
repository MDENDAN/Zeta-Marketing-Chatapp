import streamlit as st
from vertexai.generative_models import GenerativeModel
import vertexai
from google.cloud import bigquery
import os
import pandas as pd
import tempfile
import json
import plotly.express as px
import time
from google.oauth2 import service_account
from datetime import datetime

# Set page config
st.set_page_config(
    page_title="BigQuery AI Assistant",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- App styles ---
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #4285F4;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #34A853;
        margin-bottom: 0.5rem;
    }
    .info-text {
        font-size: 1rem;
        color: #666;
    }
    .highlight {
        background-color: #F1F3F4;
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# --- Initialize session state ---
if 'queries' not in st.session_state:
    st.session_state.queries = []  # Store query history
if 'credentials' not in st.session_state:
    st.session_state.credentials = None
if 'bq_client' not in st.session_state:
    st.session_state.bq_client = None
if 'model' not in st.session_state:
    st.session_state.model = None
if 'last_query_time' not in st.session_state:
    st.session_state.last_query_time = 0
if 'datasets_info' not in st.session_state:
    # Available Datasets/Table Context
    st.session_state.datasets_info = """
    Available datasets and tables and their schema field name:

    - Email_raw_dataset.hs_email_raw_data: ["Row_Updated_At","Email_name","Campaign_name","Email_subject","Campaign_type","Email_sender_name","Sender_email","createdAt","Email__Publish_date","updatedAt","createdBy_hsId","publishedBy_hsId","Email_status","Sent","Delivered","Delivered_rate","Bounced","Bounced_rate","Open","Open_rate","Click","CTR","Replies","reply_rate","Unsubscribes","Unsubscribed_rate","Click_from_computer","Click_from_mobile","Click_from_unknown_device","Open_from_computer","Open_from_mobile","Open_from_unknown_device","updatedBy_hsId","id"]
    - GA4_raw_dataset.Areawise_raw_data: ["Account_name","Property_name","Date","Session_default_channel_group","Page_path_and_screen_class","Session_campaign","Session_manual_term","Country","Region","City","New_returning","Total_users","New_users","Sessions","Engaged_sessions","User_engagement","Average_session_duration"]
     (Use this table for geographical analysis of website traffic, breaking down metrics by country, region, and city.)
    - GA4_raw_dataset.Device_category_raw_data: ["Account_name","Property_name","Date","Session_default_channel_group","Page_path_and_screen_class","Session_campaign","Session_manual_term","Session_source_medium","New_returning","Device_category","Country","Total_users","New_users","Sessions","Engaged_Sessions","User_engagement","Average_session_duration"]
    (Use this table to analyze website traffic based on the type of device used by visitors, such as mobile, desktop, or tablet.)
    - GA4_raw_dataset.Events_raw_data: ["Account_name","Property_name","Date","Session_default_channel_group","Page_path_and_screen_class","Session_campaign","Session_manual_term","Session_Manual_Ad_Content","Session_source_medium","Country","Event_Name","Event_Count"]
    (Use this table to analyze specific website interactions and events, such as form submissions, button clicks, or video plays.)
    - GA4_raw_dataset.Web_traffic_raw_data: ["Account_name","Property_name","Date","Session_default_channel_group","Page_path_and_screen_class","Session_campaign","Session_manual_term","Page_referrer","Session_source_medium","New_returning","Country","Total_users","New_users","Sessions","Engaged_sessions","User_engagement","Average_session_duration"]
    (This is the primary table for general website traffic analysis, providing insights into user behavior, traffic sources, and engagement metrics.)
    - Google_Ads_raw_dataset.Display_ads_raw_data: ["Row_Updated_At","Date","CamapignName","CamapignStatus","AdGroupName","AdGroupStatus","Impressions","Cost","Clicks"]
    (Use this table to analyze the performance of your Google Display Ads campaigns, including impressions, costs, and clicks.)
    - Google_Ads_raw_dataset.Search_keyword_performance_raw_data: ["last_updated_at","Date","Keyword","Match_type","Campaign","Ad_group_name","Ad_group_status","Impression","Interaction_rate","Cost","Search_impr_share","Clicks","Average_CPC","Quality_score","Landing_page_exp","Ad_relevance"]
    (Use this table to analyze the performance of your Google Search keywords, including impressions, costs, clicks, and quality scores.)
    - HubSpot_raw_dataset.hs_crm_accounts: ["Row_Updated_At","account_lifecycle_stage","account_manager","account_score","account_status__c","active_debit_cards___calculated","address","address2","amount_of_regular_shares___ncua","amount_of_share_drafts___ncua","annualrevenue","are_mc_softbank_investors_","asset_size__rbi_","bank_financial___deposite_credit_ratio","bank_financial_pat","bank_rank__c","bnpl_revenue_potential___calculated","card_processor","cba_member_company_2023","cba_member_company_2024","cc_loans_zt__max_of_fdic_or_ncua_","cc_outstanding","cc_purchase_transactions_credit_cards__c","cc_purchase_transactions_nilson__c","city","colloquial_name","core_processor","cost_of_funding_earning_assets___fdic","country","createdate","credit_card_issuer","credit_card_loans___000____fdic","credit_card_loans___ncua","credit_card_pv___nilson","credit_card_pv_cc_purchase_transaction__c","credit_card_tv___nilson","credit_card_value__inr_crs_","credit_card_volume","credit_cards_pv_credit_cards_nilson__c","credit_outstandings_nilson__c","credit_processing_revenue_potential___calculated","credit_processor","credit_union_rank","current___of_casa_accounts","current___of_credit_card_accounts","current___of_dda_accounts","current___of_debit_card_accounts","current___of_lending_customers","debit_card_pv___nilson","debit_card_tv___nilson","debit_cards_pv_debit_cards_nilson__c","debit_cards_pv_debit_only_transactions__c","debit_only_transactions_debit_cards__c","debit_only_transactions_nilson__c","debit_processing_revenue_potential___calculated","debit_processor","decision_platform_for_credit","decision_platform_for_credit__c","description","domain","domestic_transaction_accounts___fdic","enriched","enriched_by","equity_capital_to_assets___fdic","executive_info","existing_card_program","facebook_company_page","fdic_ncua_","fees_in_bps___calculated","fintech_priority","first_contact_createdate","first_conversion_date","first_conversion_event_name","founded_year","funding_raised_in___000","funding_status","hq_city","hq_country","hs_analytics_first_timestamp","hs_analytics_first_visit_timestamp","hs_analytics_last_timestamp","hs_analytics_last_visit_timestamp","hs_analytics_latest_source","hs_analytics_latest_source_data_1","hs_analytics_latest_source_data_2","hs_analytics_latest_source_timestamp","hs_analytics_num_page_views","hs_analytics_num_visits","hs_analytics_source","hs_analytics_source_data_1","hs_analytics_source_data_2","hs_country_code","hs_created_by_user_id","hs_is_target_account","hs_last_sales_activity_timestamp","hs_lastmodifieddate","hs_merged_object_ids","hs_num_blockers","hs_num_child_companies","hs_num_contacts_with_buying_roles","hs_num_decision_makers","hs_num_open_deals","hs_object_id","hs_object_source_label","hs_updated_by_user_id","hubspot_owner_assigneddate","hubspot_owner_id","individuals__partnerships__and_corporations___000____fdic","industry","is_public","lifecyclestage","linkedin_company_page","linkedinbio","market_segment","mobile_banking","money_market_deposit_accounts__mmdas____000____fdic","name","net_interest_margin","net_loans_and_leases___fdic","nilson__u_s__visa_mastercard_credit_card","nilson_rank__latest_report_","nilson_u_s__visa_mastercard_outstanding","nilson_u_s_visa_mastercard_credit_card__c","nilson_u_s_visa_mastercard_outstanding__c","notes_last_contacted","notes_last_updated","num_associated_contacts","num_associated_deals","num_cc_zt__max_of_nilson__fedfis_","num_contacted_notes","num_conversion_events","num_dc_zt__max_of_nilson__fedfis_____of_cu_members_","num_dc_zt_max_of_nilson_fedfis_of__c","number_of_accounts_for_regular_shares___ncua","number_of_accounts_for_share_drafts____ncua","number_of_credit_cards__nilson_","number_of_credit_cards__rbi_","number_of_dda_savings__nilson_","number_of_dda_savings__rbi_","number_of_debit_cards___ncua","number_of_debit_cards__nilson_","number_of_debit_cards__rbi_","numberofemployees","of_cu_members","orgcharthub_has_org_chart","orgcharthub_num_contacts_on_chart","orgcharthub_num_hubspot_contacts_on_chart","orgcharthub_num_placeholder_contacts_on_chart","orgcharthub_org_chart_last_updated_at","other_savings_deposits__excluding_mmdas____000____fdic","phone","pos_pg___upi_txns","pos_pg__upi_volume","primary_category","recent_conversion_date","recent_conversion_event_name","recent_deal_amount","retail_loan_book__non_psl","retail_loan_book__psl","return_on_assets__roa____fdic","return_on_equity__roe____fdic","salesforce_region","salesforceaccountid","salesforcelastsynctime","secondary_account_owner","secondary_category","size","sponsor_bank","sponsor_bank_fdic_ncua_number","state","strategy_document_link","target_classification","target_rank","target_rank__c","tier","tier__based_on_funding_","tier_level__c","timezone","total_assets___fdic","total_assets___ncua","total_assets__c","total_deposits___fdic","total_interest_income","total_loans_and_leases___000____fdic","total_loans_and_leases___ncua","total_money_raised","total_number_of_loans_and_leases___ncua","totalnumberofusers__c","twitterhandle","validated_active_consumer_cards__c","validated_consumer_debit_cards__c","web_technologies","website","yield_on_earning_assets___fdic","z_classifiers","zc_active_credit_cards__c","zc_active_debit_cards__c","zeta_arr_potential","zip","Comments"]
    - HubSpot_raw_dataset.hs_crm_individuals: ["Row_Updated_At","annualrevenue","associations_companies","available_on_website","banking","blog_banking_fintech_article_page_zeta_28678435396_subscription","city","closedate","company","company_size","compliance","contact_lifecycle_stage","contact_notes__c","contact_priority","contact_type","contact_us__score_","contact_us_form__collection_test___us","content_assets_consumed","content_assets_downloaded","content_tier","country","createdate","currentlyinworkflow","days_to_close","email","engaged","engagements_last_meeting_booked","engagements_last_meeting_booked_campaign","enriched","enriched_by","enriched_date","eski_active","eski_passive","events_registered","first_comm_date","first_conversion_date","first_conversion_event_name","first_deal_created_date","firstname","form_description","function_head","how_can_we_help_","hs_analytics_average_page_views","hs_analytics_first_timestamp","hs_analytics_first_touch_converting_campaign","hs_analytics_last_referrer","hs_analytics_last_timestamp","hs_analytics_last_touch_converting_campaign","hs_analytics_last_url","hs_analytics_num_event_completions","hs_analytics_num_page_views","hs_analytics_num_visits","hs_analytics_source","hs_analytics_source_data_1","hs_analytics_source_data_2","hs_buying_role","hs_content_membership_email","hs_content_membership_email_confirmed","hs_content_membership_notes","hs_content_membership_registration_domain_sent_to","hs_country_region_code","hs_created_by_user_id","hs_email_bad_address","hs_email_bounce","hs_email_click","hs_email_delivered","hs_email_domain","hs_email_first_click_date","hs_email_first_open_date","hs_email_first_reply_date","hs_email_first_send_date","hs_email_hard_bounce_reason_enum","hs_email_last_click_date","hs_email_last_email_name","hs_email_last_open_date","hs_email_last_reply_date","hs_email_last_send_date","hs_email_open","hs_email_optout","hs_email_optout_3513592","hs_email_optout_5113556","hs_email_optout_7455293","hs_email_optout_7904272","hs_email_quarantined","hs_email_quarantined_reason","hs_email_replied","hs_email_sends_since_last_engagement","hs_emailconfirmationstatus","hs_ip_timezone","hs_is_unworked","hs_last_sales_activity_timestamp","hs_latest_disqualified_lead_date","hs_latest_open_lead_date","hs_latest_qualified_lead_date","hs_latest_sequence_ended_date","hs_latest_sequence_enrolled","hs_latest_sequence_enrolled_date","hs_latest_source","hs_latest_source_data_1","hs_latest_source_data_2","hs_legal_basis","hs_lifecyclestage_customer_date","hs_lifecyclestage_evangelist_date","hs_lifecyclestage_lead_date","hs_lifecyclestage_marketingqualifiedlead_date","hs_lifecyclestage_opportunity_date","hs_lifecyclestage_other_date","hs_lifecyclestage_salesqualifiedlead_date","hs_lifecyclestage_subscriber_date","hs_marketable_reason_id","hs_marketable_reason_type","hs_marketable_status","hs_marketable_until_renewal","hs_merged_object_ids","hs_object_id","hs_object_source_detail_1","hs_object_source_detail_2","hs_object_source_detail_3","hs_object_source_label","hs_recent_closed_order_date","hs_registered_member","hs_registration_method","hs_sa_first_engagement_date","hs_sales_email_last_clicked","hs_sales_email_last_opened","hs_sales_email_last_replied","hs_sequences_enrolled_count","hs_sequences_is_enrolled","hs_social_facebook_clicks","hs_social_last_engagement","hs_social_linkedin_clicks","hs_social_num_broadcast_clicks","hs_social_twitter_clicks","hs_time_between_contact_creation_and_deal_close","hs_time_between_contact_creation_and_deal_creation","hs_time_to_move_from_lead_to_customer","hs_time_to_move_from_marketingqualifiedlead_to_customer","hs_time_to_move_from_opportunity_to_customer","hs_time_to_move_from_salesqualifiedlead_to_customer","hs_time_to_move_from_subscriber_to_customer","hs_timezone","hs_updated_by_user_id","hs_v2_cumulative_time_in_153180554","hs_v2_cumulative_time_in_153216287","hs_v2_cumulative_time_in_153258907","hs_v2_cumulative_time_in_153280360","hs_v2_cumulative_time_in_153288493","hs_v2_cumulative_time_in_153288494","hs_v2_cumulative_time_in_customer","hs_v2_cumulative_time_in_evangelist","hs_v2_cumulative_time_in_lead","hs_v2_cumulative_time_in_marketingqualifiedlead","hs_v2_cumulative_time_in_opportunity","hs_v2_cumulative_time_in_other","hs_v2_cumulative_time_in_salesqualifiedlead","hs_v2_cumulative_time_in_subscriber","hs_v2_date_entered_153180554","hs_v2_date_entered_153216287","hs_v2_date_entered_153258907","hs_v2_date_entered_153280360","hs_v2_date_entered_153288493","hs_v2_date_entered_153288494","hs_v2_date_entered_customer","hs_v2_date_entered_evangelist","hs_v2_date_entered_lead","hs_v2_date_entered_marketingqualifiedlead","hs_v2_date_entered_opportunity","hs_v2_date_entered_other","hs_v2_date_entered_salesqualifiedlead","hs_v2_date_entered_subscriber","hs_v2_date_exited_153180554","hs_v2_date_exited_153216287","hs_v2_date_exited_153258907","hs_v2_date_exited_153280360","hs_v2_date_exited_153288493","hs_v2_date_exited_153288494","hs_v2_date_exited_customer","hs_v2_date_exited_evangelist","hs_v2_date_exited_lead","hs_v2_date_exited_marketingqualifiedlead","hs_v2_date_exited_opportunity","hs_v2_date_exited_other","hs_v2_date_exited_salesqualifiedlead","hs_v2_date_exited_subscriber","hs_v2_latest_time_in_153180554","hs_v2_latest_time_in_153216287","hs_v2_latest_time_in_153258907","hs_v2_latest_time_in_153280360","hs_v2_latest_time_in_153288493","hs_v2_latest_time_in_153288494","hs_v2_latest_time_in_customer","hs_v2_latest_time_in_evangelist","hs_v2_latest_time_in_lead","hs_v2_latest_time_in_marketingqualifiedlead","hs_v2_latest_time_in_opportunity","hs_v2_latest_time_in_other","hs_v2_latest_time_in_salesqualifiedlead","hs_v2_latest_time_in_subscriber","hubspot_owner_assigneddate","hubspot_owner_id","hubspot_team_id","hubspotscore","ip_city","ip_country","ip_country_code","ip_state","ip_state_code","job_function","jobtitle","key_individuals","last_comm_date","lastmodifieddate","lastname","lead_intent_type","leadsource","lifecyclestage","linkedin_profile","ma","mo","mobilephone","n90d_cpl_active","n90d_cpl_passive","n90d_event_meeting","n90d_form_fill","n90d_ma","n90d_mo","n90d_tmr","n90d_tms","n90d_webinar_count","never_contacted","notes_last_contacted","notes_last_updated","notes_next_activity_date","num_associated_deals","num_contacted_notes","num_conversion_events","num_notes","num_unique_conversion_events","numemployees","phone","primary_category","qality_score","recent_conversion_date","recent_conversion_event_name","recent_deal_amount","recent_deal_close_date","recent_lead_source","secondary_category","seniority","sourced_form_intouch","state","test_event_score","time_taken_to_assign_contact_owner","tmr","tms","unengaged","webinar_attended","webinar_registered","website","zb_status","zb_sub_status","zerobouncequalityscore","tier__c","Comments","count_of_sales_email_clicked","count_of_sales_email_opened","count_of_sales_email_replied","test_dw_last_engagement_date"]
    - HubSpot_raw_dataset.Individual_Engagement: ["contact_id","associated_company_id","last_engagement_date","Date","email","firstname","lastname","hs_marketable_status","key_individuals","total_sales_email_opens","total_sales_email_clicks","total_sales_email_reply","total_marketing_email_opens","total_marketing_email_clicks","total_page_views","webinar_attended","webinar_attended_change","test_event_score","test_event_score_change","total_ma","active_form_submission","passive_form_submission","hs_analytics_last_url","today_engagement_passive","today_engagement_active"]
    - LinkedIn_Ads_raw_dataset.Li_Ads_raw_data: ["Date","Camapign_Group_name","Campaign_Group_status","Campaign_name","Campaign_type","Objective_type","Cost_type","Campaign__Daily_budget_amount","Campaign_status","Creative__Creative_name","Creative__Intended_status","Performance__Impressions","Cost__Amount_spend","Performance__Clicks"]
    (Use this table to analyze the performance of your LinkedIn Ads campaigns, including impressions, costs, and clicks.)
    - LinkedIn_Organic_raw_dataset.LinkedIn_Followers_GeoRegion_wise raw_data: ["Row_Updated_At","Report__Page","Report__Page_Id","Dimension__Market_area","Followers__Total"]
    (Use this table to analyze the geographical distribution of your LinkedIn page followers.)
    - LinkedIn_Organic_raw_dataset.LinkedIn_Followers_Contrywise raw_data: ["Row_Updated_At","Report__Page","Report__Page_Id","Dimension__Country","Followers__Total"]		
    (Use this table to analyze the country-wise distribution of your LinkedIn page followers.)
    - LinkedIn_Organic_raw_dataset.LinkedIn_organic_post_performance_raw_data:["Date","Post__Published_at","Post__Link","Post__Commentary","Performance__Impressions","Performance__Unique_impressions","Performance__Clicks","Performance__Likes","Performance__Comments","Performance__Reposts","Interactions","Engagement_rate_Interactions","Performance__Engagement_rate","distribution_targetEntities","Audience"]
    (Use this table to analyze the performance of your organic LinkedIn posts, including impressions, clicks, likes, comments, and reposts.)
    - Search_Console_Performance_Dataset.search_console_raw_data: ["date","page","query","device","country","SERP","impressions","clicks","position","type","ctr"]
    (Use this table to analyze your website's performance in Google Search results, including queries, impressions, clicks, and average position.)
    - Youtube.Video_performance_raw_data: ["Account_name","Date","Video_link","Video_published_at","Video_title","Video_description","Video_type","Video__Tags","Video__Video_duration","Performance__Views","Performance__Watch_time__minutes_","Performance__Average_view_duration","Interactions__Likes","Interactions__Shares","Interactions__Comments"]
    """

# Function to convert query to SQL using Gemini
def generate_sql(query, model):
    # Create a detailed prompt with context and examples
    prompt = f"""
You are a BigQuery SQL expert assistant that converts natural language questions into precise SQL queries.

CONTEXT:
{st.session_state.datasets_info}

USER QUESTION:
"{query}"

TASK:
1. Understand the question and identify the required tables, columns, and operations.
2. Create a well-structured, optimized BigQuery Standard SQL query.
3. Use appropriate JOINs, aggregations, and functions as needed.
4. Limit results to a reasonable number (10-100 rows) unless specified otherwise.
5. Include date filtering when appropriate, especially for time-series data.
6. Ensure proper use of GROUP BY, ORDER BY, and WHERE clauses.

Output only the SQL query without explanations. Do not include ```sql or ``` markdown tags.
"""

    response = model.generate_content(
        prompt,
        generation_config={
            "temperature": 0.2,
            "max_output_tokens": 1024,
            "top_p": 0.8,
            "top_k": 40
        }
    )
    
    # Clean up the response
    sql_query = response.text.strip()
    if sql_query.startswith("```sql"):
        sql_query = sql_query[6:].strip()
    if sql_query.endswith("```"):
        sql_query = sql_query[:-3].strip()
    
    return sql_query

# Function to execute SQL query on BigQuery
def run_query(sql_query, bq_client):
    try:
        query_job = bq_client.query(sql_query)
        results = query_job.result()
        rows = [dict(row) for row in results]
        df = pd.DataFrame(rows)
        return df, None
    except Exception as e:
        return None, str(e)

# Function to analyze and summarize results
def analyze_results(query, sql, df, model):
    # Convert DataFrame to a readable format
    if len(df) > 10:
        df_sample = df.head(10).to_string()
        total_rows = len(df)
        df_text = f"{df_sample}\n\n(Total: {total_rows} rows)"
    else:
        df_text = df.to_string()
    
    prompt = f"""
You are a data analyst expert who explains SQL query results in clear, insightful language.

USER QUESTION:
"{query}"

SQL QUERY USED:
{sql}

QUERY RESULTS (sample):
{df_text}

TASK:
1. Provide a concise 2-3 sentence summary of the key findings.
2. Mention any notable patterns, trends, or outliers.
3. If relevant, suggest one business insight or recommendation based on the data.
4. Avoid technical jargon and focus on business implications.

Make your response brief and to the point. Use bullet points if appropriate.
"""

    response = model.generate_content(
        prompt,
        generation_config={
            "temperature": 0.3,
            "max_output_tokens": 1024
        }
    )
    
    return response.text.strip()

# Function to suggest visualization based on the results
def suggest_visualization(df, sql, query, model):
    # Get DataFrame info
    df_info = {
        "columns": list(df.columns),
        "num_rows": len(df),
        "dtypes": {col: str(df[col].dtype) for col in df.columns}
    }
    
    prompt = f"""
You are a data visualization expert. Based on the SQL query and results, recommend the best visualization type.

USER QUESTION:
"{query}"

SQL QUERY:
{sql}

DATAFRAME INFO:
Columns: {df_info['columns']}
Number of rows: {df_info['num_rows']}
Column types: {df_info['dtypes']}

TASK:
Choose exactly ONE visualization type from this list and explain why it's appropriate:
- bar chart
- line chart
- scatter plot
- pie chart
- heatmap
- area chart
- histogram
- box plot
- treemap

Response format:
VISUALIZATION_TYPE: [type]
X_AXIS: [column name]
Y_AXIS: [column name]
COLOR_BY: [column name or "None"]
EXPLANATION: [1-2 sentences explaining why this visualization is appropriate]

Keep your response concise and strictly follow the format above.
"""

    response = model.generate_content(
        prompt,
        generation_config={
            "temperature": 0.2,
            "max_output_tokens": 512
        }
    )
    
    return response.text.strip()

# Function to create a visualization based on the suggestion
def create_visualization(df, viz_suggestion):
    try:
        # Parse visualization suggestion
        lines = viz_suggestion.strip().split('\n')
        viz_type = next((line.split(': ')[1] for line in lines if line.startswith('VISUALIZATION_TYPE:')), '').strip().lower()
        x_axis = next((line.split(': ')[1] for line in lines if line.startswith('X_AXIS:')), '').strip()
        y_axis = next((line.split(': ')[1] for line in lines if line.startswith('Y_AXIS:')), '').strip()
        color_by = next((line.split(': ')[1] for line in lines if line.startswith('COLOR_BY:')), 'None').strip()
        
        if color_by == 'None':
            color_by = None
            
        # Check if columns exist in DataFrame
        if x_axis not in df.columns or y_axis not in df.columns:
            return None, "Visualization error: Specified columns not found in results."
            
        # Create appropriate visualization based on type
        fig = None
        if viz_type == 'bar chart':
            fig = px.bar(df, x=x_axis, y=y_axis, color=color_by, title=f"{y_axis} by {x_axis}")
        elif viz_type == 'line chart':
            fig = px.line(df, x=x_axis, y=y_axis, color=color_by, title=f"{y_axis} over {x_axis}")
        elif viz_type == 'scatter plot':
            fig = px.scatter(df, x=x_axis, y=y_axis, color=color_by, title=f"{y_axis} vs {x_axis}")
        elif viz_type == 'pie chart':
            fig = px.pie(df, values=y_axis, names=x_axis, title=f"Distribution of {y_axis} by {x_axis}")
        elif viz_type == 'area chart':
            fig = px.area(df, x=x_axis, y=y_axis, color=color_by, title=f"{y_axis} over {x_axis}")
        elif viz_type == 'histogram':
            fig = px.histogram(df, x=x_axis, title=f"Distribution of {x_axis}")
        elif viz_type == 'box plot':
            fig = px.box(df, x=x_axis, y=y_axis, color=color_by, title=f"Distribution of {y_axis} by {x_axis}")
        elif viz_type == 'heatmap':
            # For heatmap, we need to pivot the data
            if color_by and color_by in df.columns:
                pivot_df = df.pivot_table(values=color_by, index=y_axis, columns=x_axis, aggfunc='mean')
                fig = px.imshow(pivot_df, title=f"Heatmap of {color_by} by {x_axis} and {y_axis}")
        elif viz_type == 'treemap':
            fig = px.treemap(df, path=[x_axis], values=y_axis, title=f"Treemap of {y_axis} by {x_axis}")
            
        if fig:
            fig.update_layout(height=600)
            return fig, None
        else:
            return None, "Unsupported visualization type or insufficient data."
            
    except Exception as e:
        return None, f"Visualization error: {str(e)}"

# Function to format SQL for display
def format_sql(sql):
    # A simple formatter to make SQL more readable
    keywords = ["SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "LIMIT", "UNION", "WITH"]
    formatted_sql = sql
    for keyword in keywords:
        formatted_sql = formatted_sql.replace(f" {keyword} ", f"\n{keyword} ")
    return formatted_sql

# Function to handle credential file safely
def process_credentials_file(uploaded_file):
    try:
        # Create a temporary file to store the credentials
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as temp_file:
            temp_path = temp_file.name
            temp_file.write(uploaded_file.getbuffer())
        
        # Load credentials from the temporary file
        credentials = service_account.Credentials.from_service_account_file(temp_path)
        
        # Immediately delete the temporary file after loading credentials
        os.unlink(temp_path)
        
        return credentials, None
    except Exception as e:
        return None, f"Error processing credentials: {str(e)}"

# Function to store query history
def add_to_history(query, sql, timestamp):
    # Add the query to the history
    st.session_state.queries.append({
        "query": query,
        "sql": sql,
        "timestamp": timestamp
    })
    # Keep only the last 10 queries
    if len(st.session_state.queries) > 10:
        st.session_state.queries.pop(0)

# Function to generate SQL for common questions
def get_query_suggestions():
    return [
        "What are the top 10 countries by total website sessions in the last 30 days?",
        "Compare email open rates across different campaign types",
        "What are the most popular website events by country?",
        "Show LinkedIn ad campaign performance by month",
        "What are the top search queries driving traffic to our website?"
    ]

# Main Streamlit UI layout
def main():
    # Main title
    st.markdown('<h1 class="main-header">📊 Marketing Analytics AI Assistant</h1>', unsafe_allow_html=True)
    
    # Sidebar for authentication and settings
    with st.sidebar:
        st.image("https://upload.wikimedia.org/wikipedia/commons/e/e5/Google_Cloud_logo.png", width=200)
        st.markdown("### Authentication")
        
        # File uploader for service account credentials
        uploaded_file = st.file_uploader("Upload GCP Service Account JSON", type="json")
        
        if uploaded_file and not st.session_state.credentials:
            with st.spinner("Authenticating..."):
                credentials, error = process_credentials_file(uploaded_file)
                if credentials:
                    st.session_state.credentials = credentials
                    project = credentials.project_id
                    
                    # Initialize services
                    vertexai.init(project=project, location="us-central1", credentials=credentials)
                    st.session_state.bq_client = bigquery.Client(credentials=credentials, project=project)
                    st.session_state.model = GenerativeModel(model_name="gemini-2.0-flash-001")
                    
                    st.success(f"✅ Authenticated with project: {project}")
                else:
                    st.error(f"Authentication failed: {error}")
        
        # Show query history if available
        if st.session_state.queries:
            st.markdown("### Recent Queries")
            for i, query_item in enumerate(reversed(st.session_state.queries)):
                if st.button(f"{i+1}. {query_item['query'][:30]}...", key=f"history_{i}"):
                    # Rerun the selected query
                    st.session_state.last_query = query_item['query']
                    st.experimental_rerun()
    
    # Check if authenticated
    if not st.session_state.credentials:
        st.info("👈 Please upload your Google Cloud Service Account JSON file to get started.")
        
        # Show example queries while waiting for authentication
        st.markdown('<h2 class="sub-header">Example questions you can ask:</h2>', unsafe_allow_html=True)
        for suggestion in get_query_suggestions():
            st.markdown(f"- *{suggestion}*")
            
        # Show dataset information
        with st.expander("Available datasets"):
            st.markdown(st.session_state.datasets_info)
        return
    
    # Main functionality once authenticated
    tabs = st.tabs(["Ask Questions", "Explore Data", "About"])
    
    # Ask Questions Tab
    with tabs[0]:
        # Query input and suggestions
        st.markdown('<p class="info-text">Ask any question about your marketing data</p>', unsafe_allow_html=True)
        
        # Query suggestions
        if 'last_query' not in st.session_state:
            st.session_state.last_query = ""
            
        # Display query suggestions
        col1, col2 = st.columns([3, 1])
        with col1:
            query = st.text_area("Your Question:", value=st.session_state.last_query, height=80)
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("**Try asking about:**")
            for i, suggestion in enumerate(get_query_suggestions()):
                if st.button(f"{suggestion[:20]}...", key=f"sugg_{i}"):
                    st.session_state.last_query = suggestion
                    st.experimental_rerun()
        
        # Submit button
        submit = st.button("🔍 Analyze", type="primary")
        
        # Process query when submitted
        if submit and query:
            # Check for rate limiting (no more than one query per 3 seconds)
            current_time = time.time()
            if current_time - st.session_state.last_query_time < 3:
                st.warning("Please wait a moment before submitting another query.")
            else:
                st.session_state.last_query_time = current_time
                st.session_state.last_query = query  # Save the query
                
                # Step 1: Generate SQL
                with st.spinner("Generating SQL query..."):
                    try:
                        sql_query = generate_sql(query, st.session_state.model)
                        add_to_history(query, sql_query, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    except Exception as e:
                        st.error(f"Error generating SQL: {str(e)}")
                        return
                
                # Display the generated SQL
                with st.expander("Generated SQL Query", expanded=True):
                    st.code(format_sql(sql_query), language="sql")
                
                # Step 2: Execute SQL
                with st.spinner("Executing query..."):
                    df, error = run_query(sql_query, st.session_state.bq_client)
                    if error:
                        st.error(f"Query execution failed: {error}")
                        return
                    elif df.empty:
                        st.warning("Query returned no results. Try modifying your question.")
                        return
                
                # Step 3: Analyze results
                with st.spinner("Analyzing results..."):
                    analysis = analyze_results(query, sql_query, df, st.session_state.model)
                
                # Step 4: Visualization suggestion
                with st.spinner("Creating visualization..."):
                    viz_suggestion = suggest_visualization(df, sql_query, query, st.session_state.model)
                    fig, viz_error = create_visualization(df, viz_suggestion)
                
                # Display results
                st.markdown('<h2 class="sub-header">Results</h2>', unsafe_allow_html=True)
                
                # Display analysis
                st.markdown('<div class="highlight">', unsafe_allow_html=True)
                st.markdown(f"### Key Insights\n{analysis}")
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Display visualization if available
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                elif viz_error:
                    st.info(f"Note: {viz_error}")
                
                # Display data table
                st.markdown("### Data Table")
                st.dataframe(df, use_container_width=True)
                
                # Download options
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime='text/csv',
                )
    
    # Explore Data Tab
    with tabs[1]:
        st.markdown('<h2 class="sub-header">Database Explorer</h2>', unsafe_allow_html=True)
        
        # Show dataset information
        with st.expander("Available Datasets and Tables", expanded=True):
            st.markdown(st.session_state.datasets_info)
        
        # Custom SQL input
        st.markdown("### Custom SQL Query")
        custom_sql = st.text_area("Enter your SQL query:", height=200)
        run_custom = st.button("Run Custom SQL")
        
        if run_custom and custom_sql:
            with st.spinner("Executing custom query..."):
                df, error = run_query(custom_sql, st.session_state.bq_client)
                if error:
                    st.error(f"Query execution failed: {error}")
                elif df.empty:
                    st.warning("Query returned no results.")
                else:
                    st.dataframe(df, use_container_width=True)
                    
                    # Download options
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download as CSV",
                        data=csv,
                        file_name=f"custom_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime='text/csv',
                    )
    
    # About Tab
    with tabs[2]:
        st.markdown('<h2 class="sub-header">About this Application</h2>', unsafe_allow_html=True)
        st.markdown("""
        This Marketing Analytics AI Assistant helps you analyze your marketing data using natural language queries.
        
        ### Features:
        - Ask questions in plain English about your marketing performance
        - Get instant SQL queries generated by AI
        - Visualize data automatically
        - Download results for further analysis
        
        ### How It Works:
        1. Your question is processed by Gemini AI to generate SQL
        2. The SQL query runs against your BigQuery database
        3. Results are analyzed and visualized automatically
        4. Download or explore the data further
        
        ### Security and Privacy:
        - Your credentials are processed securely and not stored
        - Queries are executed within your own Google Cloud project
        - All data remains within your BigQuery instance
        
        ### Need Help?
        For questions or support, please contact your administrator.
        """)

if __name__ == "__main__":
    main()