import pandas as pd
import requests 


def gw_stats_getter(gw):
    '''
    Summary - Returns a list of dictionaries with the stats of each player for a specified gameweek.

    Returns - List of dictionaries (each dict a player)
    '''
    url = f'https://fantasy.premierleague.com/api/event/{gw}/live/'
    req = requests.get(url).json()  

    player_list = []
    for player in req['elements']:
        player_list.append(dict(id = player['id'],gameweek = gw,**player['stats']))   
    return player_list

def gw_fixture_getter(gw):
    '''
    Summary - Function returns the next 5 fixtures for each team for each gameweek
    Returns - List of fixtures for next gameweek.
    '''
    fixture_list = []

    for i in range(1,6):
        tmp_gw = int(gw)+i
        fix_url = f'https://fantasy.premierleague.com/api/fixtures/?event={tmp_gw}'
        fix_req = requests.get(fix_url).json()
        for fixture in fix_req:

            fixture_list.append(dict(gw_to_play = i,team_id = fixture['team_h'], team_against_id=fixture['team_a'])) 
            fixture_list.append(dict(gw_to_play = i,team_id = fixture['team_a'], team_against_id=fixture['team_h'])) 


    return fixture_list


def gw_deadline_getter():
    '''
    Summary - Function returns a list of the gameweeks for the whole (current) season with their id, name
    and deadline time.

    Returns - Returns a list
    '''
    event_url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    event_req = requests.get(event_url).json()  
    deadline_list = []
    for event in event_req['events']:
        dict_ = dict(id = event['id'],gameweek = event['name'], gameweek_deadline = event['deadline_time'])
        deadline_list.append(dict_)   
    return deadline_list



def player_getter():
    '''
    Summary - Function returns a list of the gameweeks for the whole (current) season with their id, name
    and deadline time.

    Returns - Returns a list
    '''
    player_url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    player_req = requests.get(player_url).json()  
    player_list = []
    for element in player_req['elements']:
        dict_ = dict(id = element['id'], first_name = element['first_name'],second_name = element['second_name'],
            full_name = element['first_name']+' '+element['second_name'],team_id = element['team'])
        player_list.append(dict_)   
    return player_list


def team_getter():
    '''
    Summary -   

    Returns - Returns a list
    '''
    team_url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    team_req = requests.get(team_url).json()  
    team_list = []
    for team in team_req['teams']:
        team_list.append(dict(team_id = team['id'],team_name= team['name']))   
    return team_list

if __name__=='__main__':
    gw_df = pd.DataFrame(gw_fixture_getter(10))
    # team_df = pd.DataFrame(team_getter())
    # df = pd.DataFrame(gw_stats_getter(10))

    # new_df = gw_df.merge(team_df, how = 'left',left_on='team_against_id',right_on='team_id',
    #     suffixes=('','_x'))
    # new_df.drop(columns=['team_id_x','team_against_id'], inplace = True)
    # new_df.rename(columns={'team_name':'team_to_play'},inplace = True)



    
    # print(final_df.head(25))