import requests 


def gw_stats_getter(gw):
    """Returns a list of dictionaries of each players stats for the specified gameweek"""

    url = f'https://fantasy.premierleague.com/api/event/{gw}/live/'
    req = requests.get(url).json()  
    player_list = []
    for player in req['elements']:
        player_list.append(dict(id = player['id'],gameweek = gw,**player['stats']))   
    return player_list


def gw_fixture_getter(gw):
    """Returns a list of dictionaries with the fixtures for next 5 gameweeks"""

    fixture_list = []
    for i in range(1,6):
        tmp_gw = int(gw)+i
        fix_url = f'https://fantasy.premierleague.com/api/fixtures/?event={tmp_gw}'
        fix_req = requests.get(fix_url).json()
        for fixture in fix_req:
            #Add each team with the opponent they are facing (Note we want every team playing in the first col)
            fixture_list.append(dict(gw_to_play = i,team_id = fixture['team_h'], team_against_id=fixture['team_a'])) 
            fixture_list.append(dict(gw_to_play = i,team_id = fixture['team_a'], team_against_id=fixture['team_h']))
    return fixture_list


def player_getter():
    """Returns a list of dictionaries for every premier league player active this season"""

    player_url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    player_req = requests.get(player_url).json()  
    player_list = []
    for element in player_req['elements']:
        dict_ = dict(id = element['id'], first_name = element['first_name'],second_name = element['second_name'],
            full_name = element['first_name']+'     '+element['second_name'],team_id = element['team'])
        player_list.append(dict_)   
    return player_list


def team_getter():
    """Returns a list dictionaries of every team in the Premier League this season"""

    team_url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    team_req = requests.get(team_url).json()  
    team_list = []
    for team in team_req['teams']:
        team_list.append(dict(team_id = team['id'],team_name= team['name']))   
    return team_list