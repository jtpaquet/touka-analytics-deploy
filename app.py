# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime
from random import randint, choice


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
MONGODB_URI = "mongodb+srv://jtpaquet:pv9E9SB5gAVzKWbW@toukaanalytics-epm7v.gcp.mongodb.net/ToukaAnalytics?retryWrites=true&w=majority"
DBS_NAME = 'ToukaAnalytics'
FIELDS = {'content': True, 'author' : True, 'timestamp' : True, 'type' : True, 'reactions': True}

t0 = datetime.now()
connection = MongoClient(MONGODB_URI)
database = connection[DBS_NAME]
members = database['members']
messages = database['messages_8dec2021']
print('connexion time:', datetime.now()-t0)
pseudos = {author['name'] : author['pseudo'] for author in list(members.find())}
connection.close()

noms_tabs = ["Waaaaaan", "C'est l'homme comique", "Jean-Thomas Jobin approved", "Let's gooooo", "Balaladollars to the moon", "Victory royale", "Sauna", "balala", "oh nionn", "bibidi", "Mr Touka Poom sait si tu as été méchant"]
i_tab = int(str(t0.microsecond)[-1])

app = dash.Dash(__name__)
server = app.server
app.title = f'Touka Analytics - {choice(noms_tabs)}'

# Query data 


# Compile overall data on whole database

t0 = datetime.now()
data = {}

data['n_msg'] = {pseudos[d['_id']] : d['count'] for d in list(messages.aggregate([{"$sortByCount": "$author"}]))}

n_word_pipeline = [{"$match": {"content": {"$exists":True}}},{"$project": {"author": 1, "n_word": {"$size": {"$split": ["$content", " "]}}}}, {"$group" : { "_id" : "$author", "n_word" : {"$sum":"$n_word"}}}]

data['n_word'] = {pseudos[d['_id']] : d['n_word'] for d in list(messages.aggregate(n_word_pipeline))}

n_char_pipeline = [{"$match": {"content": {"$exists":True}}},{"$project": {"author": 1, "n_char": {"$strLenCP" : "$content"}}}, {"$group" : { "_id" : "$author", "n_char" : {"$sum":"$n_char"}}}]

data['n_char'] = {pseudos[d['_id']] : d['n_char'] for d in list(messages.aggregate(n_char_pipeline))}

data['ratio_char_msg'] = {name : data['n_char'][name]/data['n_msg'][name] for name in pseudos.values()}

data['ratio_word_msg'] = {name : data['n_word'][name]/data['n_msg'][name] for name in pseudos.values()}

data['total_msg'] = list(messages.aggregate( [ { "$collStats": { "storageStats": { } } } ] ))[0]['storageStats']['count']

data['date_min'] = list(messages.aggregate([{"$group":{"_id": {}, "date_min": { "$min": "$timestamp" }}}]))[0]['date_min']

data['date_max'] = list(messages.aggregate([{"$group":{"_id": {}, "date_max": { "$max": "$timestamp" }}}]))[0]['date_max']

msg_by_month_pipeline = [{"$project": {"date" : {"$toDate" : "$timestamp"}}}, {"$group" : {"_id" : { "$dateToString": { "format": "%m-%Y", "date": "$date" }}, "n_msg": {"$sum": 1}}}]

msg_by_year_pipeline = [{"$project": {"date" : {"$toDate" : "$timestamp"}}}, {"$group" : {"_id" : { "$dateToString": { "format": "%Y", "date": "$date" }}, "n_msg": {"$sum": 1}}}]

msg_by_author_by_hour_pipeline = [{"$project": {"author": 1, "date" : {"$toDate" : "$timestamp"}}}, {"$group" : {"_id" : {"author": "$author", "hour": {"$dateToString": { "format": "%H", "date": "$date" }}}, "n_msg": {"$sum": 1}}}]

msg_by_author_by_weekday_pipeline = pipeline = [{"$project": {"author": 1, "date" : {"$toDate" : "$timestamp"}}}, {"$group" : {"_id" : {"author": "$author", "weekday": {"$dateToString": { "format": "%w", "date": "$date" }}}, "n_msg": {"$sum": 1}}}]

msg_by_author_by_month_pipeline = [{"$project": {"author": 1, "date" : {"$toDate" : "$timestamp"}}}, {"$group" : {"_id" : {"author": "$author", "date": {"$dateToString": { "format": "%m-%Y", "date": "$date" }}}, "n_msg": {"$sum": 1}}}]

msg_by_author_by_year_pipeline = [{"$project": {"author": 1, "date" : {"$toDate" : "$timestamp"}}}, {"$group" : {"_id" : {"author": "$author", "year": {"$dateToString": { "format": "%Y", "date": "$date" }}}, "n_msg": {"$sum": 1}}}]

n_mot_counter_pipeline = [{"$match": {"content": {"$regex": 'nigg', '$options' : 'i'}}}, {"$project": {"author": 1, "n_mot": { "$sum": 1}}}, {"$group": {"_id": "$author", "n_mot": { "$sum": '$n_mot'}}}, {"$sort": {"n_mot": -1}}]

# Faire les groupes par hour, weekday et month

# data['n_msg_by_hour'] = {(d['_id']['author'], d['_id']['hour'] ) : d['n_msg'] for d in messages.aggregate(msg_by_author_by_hour_pipeline)}

# data['n_msg_by_weekday'] = {(d['_id']['author'], d['_id']['weekday'] ) : d['n_msg'] for d in messages.aggregate(msg_by_author_by_weekday_pipeline)}

# data['n_msg_by_month'] = {(d['_id']['author'], d['_id']['date'] ) : d['n_msg'] for d in messages.aggregate(msg_by_author_by_month_pipeline)}

# data['n_msg_by_year'] = {(d['_id']['author'], d['_id']['year'] ) :  d['n_msg'] for d in messages.aggregate(msg_by_author_by_year_pipeline)}

# data['total_msg_by_month'] = {d['_id']: d['n_msg'] for d in messages.aggregate(msg_by_month_pipeline)}

# data['total_msg_by_year'] = {d['_id']: d['n_msg'] for d in messages.aggregate(msg_by_year_pipeline)}

data['n_msg_by_hour'] = list(messages.aggregate(msg_by_author_by_hour_pipeline))
data['n_msg_by_weekday'] = list(messages.aggregate(msg_by_author_by_weekday_pipeline))
data['n_msg_by_month'] = list(messages.aggregate(msg_by_author_by_month_pipeline))
data['n_msg_by_year'] = list(messages.aggregate(msg_by_author_by_year_pipeline))
data['total_msg_by_month'] = list(messages.aggregate(msg_by_month_pipeline))
data['total_msg_by_year'] = list(messages.aggregate(msg_by_year_pipeline))
data['n_mot_counter'] = list(messages.aggregate(n_mot_counter_pipeline))

# Reactions

# react_received_by_author_pipeline = [{ "$group": {"_id": "$author", "count": {"$sum":  {"$size": "$reactions"}}} }]

# data['react_received_by_author'] = list(messages.aggregate(react_received_by_author_pipeline))

react_received_by_author_and_type_pipeline = [{"$unwind": "$reactions"}, {"$group": {"_id": {"author":"$author", "reaction": "$reactions.reaction" }, "count": {"$sum":1}}}]

data['react_received_by_author_and_type'] = list(messages.aggregate(react_received_by_author_and_type_pipeline))
react_made_by_actor_pipeline = [{"$unwind": "$reactions"}, {"$sortByCount": "$reactions.actor"}]

data['react_made_by_actor'] = list(messages.aggregate(react_made_by_actor_pipeline))
react_made_by_actor_and_reaction_pipeline = [{"$unwind": "$reactions"}, {"$group": {"_id": {"actor":"$reactions.actor", "reaction": "$reactions.reaction" }, "count": {"$sum":1}}}]

data['react_made_by_actor_and_reaction'] = list(messages.aggregate(react_made_by_actor_and_reaction_pipeline))
react_made_by_reaction_pipeline = [{"$unwind": "$reactions"}, {"$sortByCount": "$reactions.reaction"}]

data['react_made_by_reaction'] = list(messages.aggregate(react_made_by_reaction_pipeline))

print('compiling data time: ', datetime.now() - t0)

print("Data compiled")

msg_total = data["total_msg"]

fig0 = go.Figure(go.Indicator(
    mode = "number",
    value = msg_total,
    title = {"text":"Messages", "align":"center"} ))



# Bar chart msg per person

data_ = dict(sorted(data["n_msg"].items(), key=lambda item: item[1], reverse=True))
data_.pop('Charles Pilon', None)
data_.pop('Estère', None)
data_.pop('Kaven', None)
data_.pop('Marcel Leboeuf', None)
data_.pop('Pat Laf', None)
data_.pop('Champion de Catan', None)
data_.pop('Kamouk', None)
df_msg_per_person = pd.DataFrame.from_dict(data_, orient="index", columns=['Messages'])

fig1 = px.bar(df_msg_per_person, x=df_msg_per_person.index, y="Messages", title='Messages par touka', labels={
                     "index": "Touka",
                     "Messages": "Messages"})

fig1.update_layout(
    autosize=False,
    height=500
)


# Activité totale dans le temps

data_ = {}
for doc in data["total_msg_by_month"]:
    data_[doc["_id"]] = doc["n_msg"]
data_ = np.array(sorted(data_.items(), key = lambda x:datetime.strptime(x[0], '%m-%Y'), reverse=False))
x_values = [datetime.strptime(m, '%m-%Y') for m in data_[:,0]]
y_values = data_[:,1]
y_values = y_values.astype('int')
data_ = {"Temps":x_values, "Messages":y_values}
df_msg_per_month = pd.DataFrame.from_dict(data_, orient="columns")

fig2 = px.line(df_msg_per_month, x='Temps', y='Messages', title='Nombre de messages par mois')

fig2.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(step="all")
        ])
    )
)

fig2.update_layout(
    width=1400,
    height=700
)


# Bar charts msg per year

data_ = data['total_msg_by_year']
df_msg_per_year = pd.DataFrame.from_dict(data_)
fig3 = px.bar(df_msg_per_year, x='_id', y="n_msg", title='Messages par année', labels={
                     "_id": "Année",
                     "n_msg": "Messages"})

fig3.update_layout(
    autosize=False,
    height=500
)

# Bar chart reacts per person

data_ = data["react_made_by_actor"]

deleted_touka = ["Charles Pilon", "Étienne Godbout"]

data_ = list(filter(lambda d: d['_id'] not in deleted_touka, data_))

for doc in data_:
    doc["_id"] = pseudos[doc["_id"]]

df_react_per_person = pd.DataFrame.from_dict(data_)

fig4 = px.bar(df_react_per_person, x='_id', y="count", title='Réeactions par touka', labels={
                     "_id": "Touka",
                     "count": "Réactions"})

fig4.update_layout(
    autosize=False,
    height=500
)


# Bar charts n mot counter

data_ = data['n_mot_counter']

for doc in data_:
    doc["_id"] = pseudos[doc["_id"]]

df_n_mot_counter = pd.DataFrame.from_dict(data_)
fig5 = px.bar(df_n_mot_counter, x='_id', y="n_mot", title='N mot compteur :(', labels={
                     "_id": "Touka",
                     "n_mot": "N mots"})

fig5.update_layout(
    autosize=False,
    height=500
)


app.layout = html.Div(children=[
    html.Title(children="Touka Analytics"),

    html.H1(children='Touka Analytics'),

    html.H2(children='''
        Votre source de statistiques officielle sur Touka.
    '''),
    
    html.H3(children='''
        Powered by Mr. Touka Poom
    '''),
    
    dcc.Graph(
        id='msg-total',
        figure=fig0
    ),

    dcc.Graph(
        id='msg-per-touka-bar',
        figure=fig1
    ),

    dcc.Graph(
        id='msg-per-touka-time-series',
        figure=fig2
    ),

    dcc.Graph(
        id='msg-per-year',
        figure=fig3
    ),

    dcc.Graph(
        id='react-per-touka',
        figure=fig4
    ),

    dcc.Graph(
        id='n-mot-per-touka',
        figure=fig5
    )
])

if __name__ == '__main__':
    app.run_server(debug=False)