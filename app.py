import streamlit as st
from vertexai.language_models import TextGenerationModel
import vertexai
from google.cloud import bigquery
import os
from vertexai.generative_models import GenerativeModel
# --- Initialize GCP services ---
from google.auth import credentials, load_credentials_from_file
from google.oauth2 import service_account

# Streamlit file uploader to allow the user to upload the service account JSON key
uploaded_file = st.file_uploader("Upload your Google Cloud Service Account JSON", type="json")

if uploaded_file is not None:
    # Load the credentials from the uploaded file
    with open("temp_credentials.json", "wb") as f:
        f.write(uploaded_file.getbuffer())

    # Use the uploaded file for authentication
    credentials = service_account.Credentials.from_service_account_file(
        "temp_credentials.json"
    )

    # Now you can use the credentials to authenticate and make API calls
    st.write(f"Authenticated with project: {credentials.project_id}")
else:
    st.write("Please upload a service account JSON file.")


# Initialize Vertex AI and BigQuery Client
vertexai.init(project=project, location="us-central1", credentials=credentials)
bq_client = bigquery.Client(credentials= credentials, project=project)

# Load Gemini Model
model = GenerativeModel(model_name="gemini-2.0-flash-001")

# Available Datasets/Table Context
DATASETS_INFO = """
Available datasets and tables and their schema field name:

- Email_raw_dataset.hs_email_raw_data: ["Row_Updated_At","Email_name","Campaign_name","Email_subject","Campaign_type","Email_sender_name","Sender_email","createdAt","Email__Publish_date","updatedAt","createdBy_hsId","publishedBy_hsId","Email_status","Sent","Delivered","Delivered_rate","Bounced","Bounced_rate","Open","Open_rate","Click","CTR","Replies","reply_rate","Unsubscribes","Unsubscribed_rate","Click_from_computer","Click_from_mobile","Click_from_unknown_device","Open_from_computer","Open_from_mobile","Open_from_unknown_device","updatedBy_hsId","id"]
- GA4_raw_dataset.Areawise_raw_data: ["Account_name","Property_name","Date","Session_default_channel_group","Page_path_and_screen_class","Session_campaign","Session_manual_term","Country","Region","City","New_returning","Total_users","New_users","Sessions","Engaged_sessions","User_engagement","Average_session_duration"]
- GA4_raw_dataset.Device_category_raw_data: ["Account_name","Property_name","Date","Session_default_channel_group","Page_path_and_screen_class","Session_campaign","Session_manual_term","Session_source_medium","New_returning","Device_category","Country","Total_users","New_users","Sessions","Engaged_Sessions","User_engagement","Average_session_duration"]
- GA4_raw_dataset.Events_raw_data: ["Account_name","Property_name","Date","Session_default_channel_group","Page_path_and_screen_class","Session_campaign","Session_manual_term","Session_Manual_Ad_Content","Session_source_medium","Country","Event_Name","Event_Count"]
- GA4_raw_dataset.Web_traffic_raw_data: ["Account_name","Property_name","Date","Session_default_channel_group","Page_path_and_screen_class","Session_campaign","Session_manual_term","Page_referrer","Session_source_medium","New_returning","Country","Total_users","New_users","Sessions","Engaged_sessions","User_engagement","Average_session_duration"]
- Google_Ads_raw_dataset.Display_ads_raw_data: ["Row_Updated_At","Date","CamapignName","CamapignStatus","AdGroupName","AdGroupStatus","Impressions","Cost","Clicks"]
- Google_Ads_raw_dataset.Search_keyword_performance_raw_data: ["last_updated_at","Date","Keyword","Match_type","Campaign","Ad_group_name","Ad_group_status","Impression","Interaction_rate","Cost","Search_impr_share","Clicks","Average_CPC","Quality_score","Landing_page_exp","Ad_relevance"]
- HubSpot_raw_dataset.hs_crm_accounts: ["Row_Updated_At","account_lifecycle_stage","account_manager","account_score","account_status__c","active_debit_cards___calculated","address","address2","amount_of_regular_shares___ncua","amount_of_share_drafts___ncua","annualrevenue","are_mc_softbank_investors_","asset_size__rbi_","bank_financial___deposite_credit_ratio","bank_financial_pat","bank_rank__c","bnpl_revenue_potential___calculated","card_processor","cba_member_company_2023","cba_member_company_2024","cc_loans_zt__max_of_fdic_or_ncua_","cc_outstanding","cc_purchase_transactions_credit_cards__c","cc_purchase_transactions_nilson__c","city","colloquial_name","core_processor","cost_of_funding_earning_assets___fdic","country","createdate","credit_card_issuer","credit_card_loans___000____fdic","credit_card_loans___ncua","credit_card_pv___nilson","credit_card_pv_cc_purchase_transaction__c","credit_card_tv___nilson","credit_card_value__inr_crs_","credit_card_volume","credit_cards_pv_credit_cards_nilson__c","credit_outstandings_nilson__c","credit_processing_revenue_potential___calculated","credit_processor","credit_union_rank","current___of_casa_accounts","current___of_credit_card_accounts","current___of_dda_accounts","current___of_debit_card_accounts","current___of_lending_customers","debit_card_pv___nilson","debit_card_tv___nilson","debit_cards_pv_debit_cards_nilson__c","debit_cards_pv_debit_only_transactions__c","debit_only_transactions_debit_cards__c","debit_only_transactions_nilson__c","debit_processing_revenue_potential___calculated","debit_processor","decision_platform_for_credit","decision_platform_for_credit__c","description","domain","domestic_transaction_accounts___fdic","enriched","enriched_by","equity_capital_to_assets___fdic","executive_info","existing_card_program","facebook_company_page","fdic_ncua_","fees_in_bps___calculated","fintech_priority","first_contact_createdate","first_conversion_date","first_conversion_event_name","founded_year","funding_raised_in___000","funding_status","hq_city","hq_country","hs_analytics_first_timestamp","hs_analytics_first_visit_timestamp","hs_analytics_last_timestamp","hs_analytics_last_visit_timestamp","hs_analytics_latest_source","hs_analytics_latest_source_data_1","hs_analytics_latest_source_data_2","hs_analytics_latest_source_timestamp","hs_analytics_num_page_views","hs_analytics_num_visits","hs_analytics_source","hs_analytics_source_data_1","hs_analytics_source_data_2","hs_country_code","hs_created_by_user_id","hs_is_target_account","hs_last_sales_activity_timestamp","hs_lastmodifieddate","hs_merged_object_ids","hs_num_blockers","hs_num_child_companies","hs_num_contacts_with_buying_roles","hs_num_decision_makers","hs_num_open_deals","hs_object_id","hs_object_source_label","hs_updated_by_user_id","hubspot_owner_assigneddate","hubspot_owner_id","individuals__partnerships__and_corporations___000____fdic","industry","is_public","lifecyclestage","linkedin_company_page","linkedinbio","market_segment","mobile_banking","money_market_deposit_accounts__mmdas____000____fdic","name","net_interest_margin","net_loans_and_leases___fdic","nilson__u_s__visa_mastercard_credit_card","nilson_rank__latest_report_","nilson_u_s__visa_mastercard_outstanding","nilson_u_s_visa_mastercard_credit_card__c","nilson_u_s_visa_mastercard_outstanding__c","notes_last_contacted","notes_last_updated","num_associated_contacts","num_associated_deals","num_cc_zt__max_of_nilson__fedfis_","num_contacted_notes","num_conversion_events","num_dc_zt__max_of_nilson__fedfis_____of_cu_members_","num_dc_zt_max_of_nilson_fedfis_of__c","number_of_accounts_for_regular_shares___ncua","number_of_accounts_for_share_drafts____ncua","number_of_credit_cards__nilson_","number_of_credit_cards__rbi_","number_of_dda_savings__nilson_","number_of_dda_savings__rbi_","number_of_debit_cards___ncua","number_of_debit_cards__nilson_","number_of_debit_cards__rbi_","numberofemployees","of_cu_members","orgcharthub_has_org_chart","orgcharthub_num_contacts_on_chart","orgcharthub_num_hubspot_contacts_on_chart","orgcharthub_num_placeholder_contacts_on_chart","orgcharthub_org_chart_last_updated_at","other_savings_deposits__excluding_mmdas____000____fdic","phone","pos_pg___upi_txns","pos_pg__upi_volume","primary_category","recent_conversion_date","recent_conversion_event_name","recent_deal_amount","retail_loan_book__non_psl","retail_loan_book__psl","return_on_assets__roa____fdic","return_on_equity__roe____fdic","salesforce_region","salesforceaccountid","salesforcelastsynctime","secondary_account_owner","secondary_category","size","sponsor_bank","sponsor_bank_fdic_ncua_number","state","strategy_document_link","target_classification","target_rank","target_rank__c","tier","tier__based_on_funding_","tier_level__c","timezone","total_assets___fdic","total_assets___ncua","total_assets__c","total_deposits___fdic","total_interest_income","total_loans_and_leases___000____fdic","total_loans_and_leases___ncua","total_money_raised","total_number_of_loans_and_leases___ncua","totalnumberofusers__c","twitterhandle","validated_active_consumer_cards__c","validated_consumer_debit_cards__c","web_technologies","website","yield_on_earning_assets___fdic","z_classifiers","zc_active_credit_cards__c","zc_active_debit_cards__c","zeta_arr_potential","zip","Comments"]
- HubSpot_raw_dataset.hs_crm_individuals: ["Row_Updated_At","annualrevenue","associations_companies","available_on_website","banking","blog_banking_fintech_article_page_zeta_28678435396_subscription","city","closedate","company","company_size","compliance","contact_lifecycle_stage","contact_notes__c","contact_priority","contact_type","contact_us__score_","contact_us_form__collection_test___us","content_assets_consumed","content_assets_downloaded","content_tier","country","createdate","currentlyinworkflow","days_to_close","email","engaged","engagements_last_meeting_booked","engagements_last_meeting_booked_campaign","enriched","enriched_by","enriched_date","eski_active","eski_passive","events_registered","first_comm_date","first_conversion_date","first_conversion_event_name","first_deal_created_date","firstname","form_description","function_head","how_can_we_help_","hs_analytics_average_page_views","hs_analytics_first_timestamp","hs_analytics_first_touch_converting_campaign","hs_analytics_last_referrer","hs_analytics_last_timestamp","hs_analytics_last_touch_converting_campaign","hs_analytics_last_url","hs_analytics_num_event_completions","hs_analytics_num_page_views","hs_analytics_num_visits","hs_analytics_source","hs_analytics_source_data_1","hs_analytics_source_data_2","hs_buying_role","hs_content_membership_email","hs_content_membership_email_confirmed","hs_content_membership_notes","hs_content_membership_registration_domain_sent_to","hs_country_region_code","hs_created_by_user_id","hs_email_bad_address","hs_email_bounce","hs_email_click","hs_email_delivered","hs_email_domain","hs_email_first_click_date","hs_email_first_open_date","hs_email_first_reply_date","hs_email_first_send_date","hs_email_hard_bounce_reason_enum","hs_email_last_click_date","hs_email_last_email_name","hs_email_last_open_date","hs_email_last_reply_date","hs_email_last_send_date","hs_email_open","hs_email_optout","hs_email_optout_3513592","hs_email_optout_5113556","hs_email_optout_7455293","hs_email_optout_7904272","hs_email_quarantined","hs_email_quarantined_reason","hs_email_replied","hs_email_sends_since_last_engagement","hs_emailconfirmationstatus","hs_ip_timezone","hs_is_unworked","hs_last_sales_activity_timestamp","hs_latest_disqualified_lead_date","hs_latest_open_lead_date","hs_latest_qualified_lead_date","hs_latest_sequence_ended_date","hs_latest_sequence_enrolled","hs_latest_sequence_enrolled_date","hs_latest_source","hs_latest_source_data_1","hs_latest_source_data_2","hs_legal_basis","hs_lifecyclestage_customer_date","hs_lifecyclestage_evangelist_date","hs_lifecyclestage_lead_date","hs_lifecyclestage_marketingqualifiedlead_date","hs_lifecyclestage_opportunity_date","hs_lifecyclestage_other_date","hs_lifecyclestage_salesqualifiedlead_date","hs_lifecyclestage_subscriber_date","hs_marketable_reason_id","hs_marketable_reason_type","hs_marketable_status","hs_marketable_until_renewal","hs_merged_object_ids","hs_object_id","hs_object_source_detail_1","hs_object_source_detail_2","hs_object_source_detail_3","hs_object_source_label","hs_recent_closed_order_date","hs_registered_member","hs_registration_method","hs_sa_first_engagement_date","hs_sales_email_last_clicked","hs_sales_email_last_opened","hs_sales_email_last_replied","hs_sequences_enrolled_count","hs_sequences_is_enrolled","hs_social_facebook_clicks","hs_social_last_engagement","hs_social_linkedin_clicks","hs_social_num_broadcast_clicks","hs_social_twitter_clicks","hs_time_between_contact_creation_and_deal_close","hs_time_between_contact_creation_and_deal_creation","hs_time_to_move_from_lead_to_customer","hs_time_to_move_from_marketingqualifiedlead_to_customer","hs_time_to_move_from_opportunity_to_customer","hs_time_to_move_from_salesqualifiedlead_to_customer","hs_time_to_move_from_subscriber_to_customer","hs_timezone","hs_updated_by_user_id","hs_v2_cumulative_time_in_153180554","hs_v2_cumulative_time_in_153216287","hs_v2_cumulative_time_in_153258907","hs_v2_cumulative_time_in_153280360","hs_v2_cumulative_time_in_153288493","hs_v2_cumulative_time_in_153288494","hs_v2_cumulative_time_in_customer","hs_v2_cumulative_time_in_evangelist","hs_v2_cumulative_time_in_lead","hs_v2_cumulative_time_in_marketingqualifiedlead","hs_v2_cumulative_time_in_opportunity","hs_v2_cumulative_time_in_other","hs_v2_cumulative_time_in_salesqualifiedlead","hs_v2_cumulative_time_in_subscriber","hs_v2_date_entered_153180554","hs_v2_date_entered_153216287","hs_v2_date_entered_153258907","hs_v2_date_entered_153280360","hs_v2_date_entered_153288493","hs_v2_date_entered_153288494","hs_v2_date_entered_customer","hs_v2_date_entered_evangelist","hs_v2_date_entered_lead","hs_v2_date_entered_marketingqualifiedlead","hs_v2_date_entered_opportunity","hs_v2_date_entered_other","hs_v2_date_entered_salesqualifiedlead","hs_v2_date_entered_subscriber","hs_v2_date_exited_153180554","hs_v2_date_exited_153216287","hs_v2_date_exited_153258907","hs_v2_date_exited_153280360","hs_v2_date_exited_153288493","hs_v2_date_exited_153288494","hs_v2_date_exited_customer","hs_v2_date_exited_evangelist","hs_v2_date_exited_lead","hs_v2_date_exited_marketingqualifiedlead","hs_v2_date_exited_opportunity","hs_v2_date_exited_other","hs_v2_date_exited_salesqualifiedlead","hs_v2_date_exited_subscriber","hs_v2_latest_time_in_153180554","hs_v2_latest_time_in_153216287","hs_v2_latest_time_in_153258907","hs_v2_latest_time_in_153280360","hs_v2_latest_time_in_153288493","hs_v2_latest_time_in_153288494","hs_v2_latest_time_in_customer","hs_v2_latest_time_in_evangelist","hs_v2_latest_time_in_lead","hs_v2_latest_time_in_marketingqualifiedlead","hs_v2_latest_time_in_opportunity","hs_v2_latest_time_in_other","hs_v2_latest_time_in_salesqualifiedlead","hs_v2_latest_time_in_subscriber","hubspot_owner_assigneddate","hubspot_owner_id","hubspot_team_id","hubspotscore","ip_city","ip_country","ip_country_code","ip_state","ip_state_code","job_function","jobtitle","key_individuals","last_comm_date","lastmodifieddate","lastname","lead_intent_type","leadsource","lifecyclestage","linkedin_profile","ma","mo","mobilephone","n90d_cpl_active","n90d_cpl_passive","n90d_event_meeting","n90d_form_fill","n90d_ma","n90d_mo","n90d_tmr","n90d_tms","n90d_webinar_count","never_contacted","notes_last_contacted","notes_last_updated","notes_next_activity_date","num_associated_deals","num_contacted_notes","num_conversion_events","num_notes","num_unique_conversion_events","numemployees","phone","primary_category","qality_score","recent_conversion_date","recent_conversion_event_name","recent_deal_amount","recent_deal_close_date","recent_lead_source","secondary_category","seniority","sourced_form_intouch","state","test_event_score","time_taken_to_assign_contact_owner","tmr","tms","unengaged","webinar_attended","webinar_registered","website","zb_status","zb_sub_status","zerobouncequalityscore","tier__c","Comments","count_of_sales_email_clicked","count_of_sales_email_opened","count_of_sales_email_replied","test_dw_last_engagement_date"]
- HubSpot_raw_dataset.Individual_Engagement: ["contact_id","associated_company_id","last_engagement_date","Date","email","firstname","lastname","hs_marketable_status","key_individuals","total_sales_email_opens","total_sales_email_clicks","total_sales_email_reply","total_marketing_email_opens","total_marketing_email_clicks","total_page_views","webinar_attended","webinar_attended_change","test_event_score","test_event_score_change","total_ma","active_form_submission","passive_form_submission","hs_analytics_last_url","today_engagement_passive","today_engagement_active"]
- LinkedIn_Ads_raw_dataset.LinkedIn_Ads_raw_data: ["Date","Camapign_Group_name","Campaign_Group_status","Campaign_name","Campaign_type","Objective_type","Cost_type","Campaign__Daily_budget_amount","Campaign_status","Creative__Creative_name","Creative__Intended_status","Performance__Impressions","Cost__Amount_spend","Performance__Clicks"]
- LinkedIn_Organic_raw_dataset.LinkedIn_organic_post_performance_raw_data:["Date","Post__Published_at","Post__Link","Post__Commentary","Performance__Impressions","Performance__Unique_impressions","Performance__Clicks","Performance__Likes","Performance__Comments","Performance__Reposts","Interactions","Engagement_rate_Interactions","Performance__Engagement_rate","distribution_targetEntities","Audience"]
- Search_Console_Performance_Dataset.search_console_raw_data: ["date","page","query","device","country","SERP","impressions","clicks","position","type","ctr"]
- Youtube.Video_performance_raw_data: ["Account_name","Date","Video_link","Video_published_at","Video_title","Video_description","Video_type","Video__Tags","Video__Video_duration","Performance__Views","Performance__Watch_time__minutes_","Performance__Average_view_duration","Interactions__Likes","Interactions__Shares","Interactions__Comments"]
"""

# --- Streamlit App ---

st.title("Ask your BigQuery Database 🧠🔍")

user_query = st.text_input("Enter your question:")
submit_button = st.button("Ask")

if submit_button and user_query:
    with st.spinner('Generating SQL...'):

        # 1. Generate SQL from Gemini
        prompt = f"""
You are a BigQuery SQL expert.
Available datasets and tables: {DATASETS_INFO}
User question: "{user_query}"
Write a BigQuery Standard SQL query that correctly answers it.
Only output the SQL code without any explanation.
"""

        response = model.generate_content(prompt,generation_config={
        "temperature": 0,
        "max_output_tokens": 512
    }
)
        sql_query = response.text.replace("```sql", "").replace("```", "").strip()

        st.subheader("🔹 Generated SQL:")
        st.code(sql_query)

    with st.spinner('Running Query on BigQuery...'):
        try:
            query_job = bq_client.query(sql_query)
            rows = [dict(row) for row in query_job.result()]
            if not rows:
                st.warning("No data found for your query!")
                rows = []
        except Exception as e:
            st.error(f"Error executing SQL: {str(e)}")
            rows = []

    if rows:
        st.subheader("🔹 Query Result:")
        st.dataframe(rows)

        with st.spinner('Summarizing Results...'):
            # 2. Summarize the Query Results
            summary_prompt = f"Summarize these query results in 2-3 sentences:\n{rows}"
            summary_response = model.generate_content(summary_prompt,generation_config={"temperature":0,"max_output_tokens":512})
            summary_text = summary_response.text.strip()

            st.subheader("🔹 Summary:")
            st.success(summary_text)