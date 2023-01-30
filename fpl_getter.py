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
        player_list.append(dict(id = player['id'],**player['stats']))   
    return player_list

def gw_fixture_getter(deadline):
    '''
    Summary - Function returns the list of fixtures for the next gameweek.

    Returns - List of fixtures for next gameweek.
    '''

    fix_url = 'https://fantasy.premierleague.com/api/fixtures/?future=1'
    fix_req = requests.get(fix_url).json()

    fixture_list = []
    for fixture in fix_req:
        if fixture['kickoff_time'] > deadline:
            break
        fixture_list.append(fixture)  
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
            team = element['team'], team_code = element['team_code'])
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
        team_list.append(team)   
    return team_list

