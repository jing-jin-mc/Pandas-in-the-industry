
######## Libraries ###############
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import glob

#### Global Variables ################
#### The percentile threshold 
trust_worthy_p_thresh = 50
avg_rating_p_thresh = 50

#### Add time columns to the dataframe from the csv files
def get_time_labels(df):
    df["created"] = pd.to_datetime(df["created"],format="%Y%m%d %H:%M:%S")
    df["year"] = df["created"].dt.year
    df["month"] = df["created"].apply(lambda x: '{:02d}'.format(x.month))
    df["yymm"] = df.apply(lambda x: str(x["year"])+str(x["month"]), axis=1)
    ##### or using pd.Period() ######
    ## df["yymm"] = df['created'].apply(lambda x: pd.Period(x,freq='M').strftime('%Y%m'))
    return df


#### Calculate the trust worthy value for each agent 
def calculate_trust_worthy(df):
    agent_total_invitations = df[df["status"].isin([1,2,3,4])
                                ][["agent_id",
                                   "id"]
                                 ].groupby("agent_id"
                                          ).count().rename(columns = {"id":"number_total"}
                                                          ).reset_index()
    agent_sent_invitations = df[df["status"].isin([2])
                               ][["agent_id",
                                  "id"]
                                ].groupby("agent_id"
                                         ).count().rename(columns = {"id":"number_sent"}
                                                         ).reset_index()
    agent_invitations = agent_total_invitations.merge(agent_sent_invitations,
                                                      left_on = "agent_id", 
                                                      right_on = "agent_id",
                                                      how = 'left')
    
    agent_invitations = agent_invitations.fillna(0)
    agent_invitations["trust_worthy"] = agent_invitations.apply(lambda x: x["number_sent"]/x["number_total"] 
                                                                if x["number_total"]>0 
                                                                else 0,
                                                                axis = 1)
    agent_invitations = agent_invitations.sort_values(by=["trust_worthy",
                                                          "number_total"],
                                                      ascending = [False,False]
                                                     )
    return agent_invitations


#### Calculate the most trust worth agent for the each within the selected time range and geo range 
def most_trust_worthy(df):
    thresh = np.percentile(df["number_total"], trust_worthy_p_thresh)
    df = df[df["number_total"]>=thresh]
    result = df.sort_values(by = ["trust_worthy","number_total"],ascending = False)
    return result.iloc[0]



#### Calculate the number of reviews the agent gets
def calculate_no_reviews(df):
    agent_reviews = df[df["status"].isin([3])
                      ][["agent_id",
                         "invite_id"]
                       ].groupby("agent_id"
                                ).count().rename(columns = {"invite_id":"number_reviews"}
                                                ).reset_index()
    return agent_reviews



#### Calculate the agent with most reviews within the selected time range and geo range 
def most_reviews(df):
    result = df.sort_values(by = ["number_reviews"],ascending = False)
    return result.iloc[0]



#### Calculate the average rating for the agent
def calculate_avg_rating(df):
    df = df.dropna()
    tmp = df.groupby("agent_id").agg({"rating":np.mean,
                                      "invite_id":lambda x:x.count()}).reset_index()
    tmp = tmp.rename(columns={"invite_id":"number_reviews",
                                 "rating":"avg_rating"})
    return tmp



#### Calculate the agent with highest avg-rating within the selected time range and geo range 
def highest_avg_rate(df):
    thresh = np.percentile(df["number_reviews"], avg_rating_p_thresh)
    df = df[df["number_reviews"]>=thresh]
    #print ("The Thresh is %d reviews"%thresh)
    result = df.sort_values(by = ["avg_rating","number_reviews"],ascending = False)
    return result.iloc[0]



#### Add more detailed agent information for the traget dataframe 
def add_agent_info(target,info,target_key,info_key):
    target = target.dropna()
    result = target.merge(info,left_on = target_key, right_on = info_key,how = 'left')
    result = result[result["employeeState"]=="PUBLISHED"]
    return result




#### Add the geo type for the geo_id
def find_geo_type(row):
    if str(row).startswith("3"):
        return 3
    elif str(row).startswith("4"):
        return 4
    elif str(row).startswith("5"):
        return 5
    elif str(row).startswith("6"):
        return 6
    else:
        return 0


################### No need to run every time, result can be saved another CSV file #############################
#### Add geo information for the agents #######################
def add_geo_info_for_agent(zipcode,location,agent_info):
    ###### Used for scalar down the agent 
    #### zipcode.csv has zipcode, location_id
    zipcode_location_id = pd.DataFrame(zipcode['locations'].map(lambda x: str(x)[1:-1]).str.split(',').tolist(),
                                       index=zipcode['zip_code']).stack().reset_index()
    zipcode_location_id= zipcode_location_id[["zip_code",0]].rename(columns = {0:"location_id"})
    zipcode_location_id["geo_type"] = zipcode_location_id["location_id"].apply(find_geo_type)
    
    #### Create a geo-book for geo information in location.csv
    #### location.csv has identifier for each location_id
    location["location_id"] = location["location_id"].astype(str)
    zipcode_target = zipcode_location_id[(zipcode_location_id["geo_type"]==5) | (zipcode_location_id["geo_type"]==6)]
    zipcode_target = zipcode_target.merge(location[["location_id",
                                                    "identifier"]
                                                  ],
                                          on = ["location_id"],
                                          how = 'left')    
    
    #### zipcode is the key to join the agent_info to the locations at type =5 and 6
    agent_info = agent_info.rename(columns={"postalCode":"zip_code"})
    agent_info_geo = agent_info.merge(zipcode_target,
                                      on = ["zip_code"],
                                      how = 'left')
    # add country level 
    agent_info_geo["country"] = "Sweden"
    return agent_info_geo 



################### No need to run every time, result can be saved another CSV file #############################
#### Generate a geo dictionary for all the agents
def calculate_agent_geo_level(agent_info_geo):
    ####### Country level ##############
    tmp = agent_info_geo.drop_duplicates(subset=["agent_id"], keep='first',inplace=False)
    agent_geo_level = {"country":tmp[["agent_id","country","employeeState"]]}
    ####### City level ##############
    tmp = agent_info_geo.drop_duplicates(subset=["agent_id","city"], keep='first',inplace=False)
    agent_geo_level.update({"city":tmp[["agent_id","city","employeeState"]]})
    ####### Geo_5 level ##############
    tmp = agent_info_geo[(agent_info_geo["city"]=="stockholm") &
                         (agent_info_geo["geo_type"]== 5)].drop_duplicates(subset=["agent_id"],
                                                                           keep='first',inplace=False)
    tmp = tmp.rename(columns={"identifier":"geo_5_name"})
    agent_geo_level.update({"geo_5_name":tmp[["agent_id","geo_5_name","employeeState"]]})
    ####### Geo_6 level ##############
    tmp = agent_info_geo[(agent_info_geo["city"]=="stockholm")&
                         (agent_info_geo["geo_type"]== 6)].drop_duplicates(subset=["agent_id"],
                                                                           keep='first',inplace=False)
    tmp = tmp.rename(columns={"identifier":"geo_6_name"})
    agent_geo_level.update({"geo_6_name":tmp[["agent_id","geo_6_name","employeeState"]]})
    
    return agent_geo_level



#### Generate a time-range dictionary for interested time ranges 
def calculate_time_range_dic(df):
    time_now = df["created"].max()
    mm_list = ["01","02","03","04","05","06","07","08","09","10","11","12"]
    
    last_year = [str(time_now.year - 1)+str(i) for i in mm_list]
    time_range_dic = {"LastYear":last_year}
    
    if time_now.month >= 7:
        half_year = [str(time_now.year) + str(i) for i in mm_list[:6]]
    else:
        half_year = [str(time_now.year-1) + str(i) for i in mm_list[6:]]    
    time_range_dic.update({"HalfYear":half_year})

    if (time_now.month > 9):
        Q1 = [str(time_now.year) + str(i) for i in mm_list[:3]]
        Q2 = [str(time_now.year) + str(i) for i in mm_list[3:6]]
        Q3 = [str(time_now.year) + str(i) for i in mm_list[6:9]]
        time_range_dic.update({"Q1":Q1,"Q2":Q2,"Q3":Q3})
    elif (time_now.month > 6):
        Q1 = [str(time_now.year) + str(i) for i in mm_list[:3]]
        Q2 = [str(time_now.year) + str(i) for i in mm_list[3:6]]
        time_range_dic.update({"Q1":Q1,"Q2":Q2})
    elif (time_now.month > 3):
        Q1 = [str(time_now.year) + str(i) for i in mm_list[:3]]
        time_range_dic.update({"Q1":Q1})
    else:
        Q4 = [str(time_now.year-1) + str(i) for i in mm_list[9:]]
        time_range_dic.update({"Q4":Q4})
    
    if (time_now.month < 2):
        last_month = [str(time_now.year-1) + "12"]
    else:
        last_month = [str(time_now.year) + '{:02d}'.format(time_now.month-1)]
    time_range_dic.update({"LastMonth":last_month})   
    return time_range_dic



########## Three functions to generate the badges ###########
def get_trust_worth_list(prepared_invite_for_agents,time_range,time_key,agent_geo,geo_key):
    tmp_prepared_invites = prepared_invite_for_agents[prepared_invite_for_agents["yymm"].
                                                      isin(time_range
                                                          )][["agent_id",
                                                              "id",
                                                              "status"]]
    trust_worthy = calculate_trust_worthy(tmp_prepared_invites)
    agent_trust_worthy = add_agent_info(trust_worthy,
                                        agent_geo,
                                        "agent_id",
                                        "agent_id")
    tmp_badge = agent_trust_worthy.groupby(geo_key).apply(most_trust_worthy)
    #print (tmp_badge)
    tmp_result = tmp_badge[["agent_id","trust_worthy"]]
    tmp_result["time_scope"] = time_key 
    tmp_result["geo_scope"] = geo_key
    tmp_result["badge_type"] = "MostTrustWorthy"
    tmp_result.columns = ["agent_id","badgeValue","time_scope","geo_scope","badge_type"]
    return tmp_result

def get_most_review_list(invites,time_range,time_key,agent_geo,geo_key):
    tmp_invites = invites[invites["yymm"].isin(time_range
                                              )][["agent_id",
                                                  "invite_id",
                                                  "status"]]
    if tmp_invites.empty:
        return None
    else:
        num_reviews = calculate_no_reviews(tmp_invites)
        #print (num_reviews)
        agent_invitations = add_agent_info(num_reviews,
                                           agent_geo,
                                           "agent_id",
                                           "agent_id")
        tmp_badge = agent_invitations.groupby(geo_key).apply(most_reviews)
        #print (tmp_badge)
        tmp_result = tmp_badge[["agent_id","number_reviews"]]
        tmp_result["time_scope"] = time_key 
        tmp_result["geo_scope"] = geo_key
        tmp_result["badge_type"] = "MostReviews"
        tmp_result.columns = ["agent_id","badgeValue","time_scope","geo_scope","badge_type"]
        return tmp_result

def get_highest_rate_list(reviews,time_range,time_key,agent_geo,geo_key):
    tmp_reviews = reviews[reviews["yymm"].isin(time_range
                                              )][["agent_id",
                                                  "invite_id",
                                                  "rating"]]
    if tmp_reviews.empty:
        return None
    else:
        avg_rating = calculate_avg_rating(tmp_reviews)
        agent_reviews = add_agent_info(avg_rating,
                                       agent_geo,
                                       "agent_id",
                                       "agent_id")

        tmp_badge = agent_reviews.groupby(geo_key).apply(highest_avg_rate)
        tmp_result = tmp_badge[["agent_id","avg_rating"]]
        tmp_result["time_scope"] = time_key 
        tmp_result["geo_scope"] = geo_key
        tmp_result["badge_type"] = "HighestRate"
        tmp_result.columns = ["agent_id","badgeValue","time_scope","geo_scope","badge_type"]
        return tmp_result

def get_dic(agents,location,zipcode,prepared_invite_for_agents):
    agents['city'] = agents['city'].str.lower()
    agents['city'] = agents['city'].str.strip()
    agent_info = agents[['city','agent_id','name','employeeState','venue_id','postalCode']]
  
    agent_info_geo = add_geo_info_for_agent(zipcode,location,agent_info)
    ### Calculte the geo-dictionary for all the agents
    agent_geo_dic = calculate_agent_geo_level(agent_info_geo)
    #### Generate a time-range dictionary for interested time ranges 
    time_range_dic = calculate_time_range_dic(prepared_invite_for_agents)
    return agent_geo_dic,time_range_dic