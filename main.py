
######## Libraries ###############
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import glob
import warnings
warnings.filterwarnings("ignore")

from functions import *


# ### Read files and add necessary extra columns

# ### Add Geo-information for the agent 

# ## Loop over all the time range and geo for three types of badges

def main(time_range_dic,agent_geo_dic,prepared_invite_for_agents,invites,reviews,filename):
    badges = pd.DataFrame()
    for time_key in time_range_dic:
        time_range = time_range_dic[time_key]
        #print (time_range)
        for geo_key in agent_geo_dic:
            agent_geo = agent_geo_dic[geo_key]
            #print (geo_key)
            ################# Trust_worthy ##############
            tmp_result = get_trust_worth_list(prepared_invite_for_agents,time_range,time_key,agent_geo,geo_key)
            badges = badges.append(tmp_result)
            ################ Most reviews ###############
            tmp_result = get_most_review_list(invites,time_range,time_key,agent_geo,geo_key)
            badges = badges.append(tmp_result)
            ################ Highest rate ###############
            tmp_result = get_highest_rate_list(reviews,time_range,time_key,agent_geo,geo_key)
            badges = badges.append(tmp_result)    
    badges = badges.reset_index().rename(columns={"index":"geo_scope_name"})
    badges["agent_id"] = badges["agent_id"].astype(int)
    badges["badgeValue"] = badges["badgeValue"].round(4)
    badges.to_csv(filename,index=False)
    print ("Results have been save to "+ filename)

if __name__ == '__main__':
    prepared_invite_for_agents = pd.read_csv("prepared_invite_for_agents.csv")
    print ("There are %d prepared invites for agents"%len(prepared_invite_for_agents))
    prepared_invite_for_agents = get_time_labels(prepared_invite_for_agents)
    #prepared_invite_for_agents.head()

    invites = pd.read_csv("invites.csv")
    invites = get_time_labels(invites)
    print ("There are %d invites"%len(invites))
    #invites.head()

    reviews = pd.read_csv("reviews.csv")
    reviews = get_time_labels(reviews)
    reviews = reviews.rename(columns={"agentid":"agent_id"})
    print ("There are %d reviews"%len(reviews))
    #reviews.head()

    ### Read in agent info and select intersted parts
    agents = pd.read_csv("agents.csv")
    print ("There are %d agents"%len(agents))
    
    location = pd.read_csv("location.csv")
    ### Drop the row whose "county","municipality","locality" value is NaN
    zipcode = pd.read_csv("zipcode.csv").dropna(subset=["county","municipality","locality"])
    
    agent_geo_dic,time_range_dic = get_dic(agents,location,zipcode,prepared_invite_for_agents)
    
    main(time_range_dic,agent_geo_dic,prepared_invite_for_agents,invites,reviews,"badges.csv")
