from SPARQLBurger.SPARQLQueryBuilder import *
import requests
import pandas as pd
import pymongo

file = 'movies_metadata.csv'
df = pd.read_csv(file)
df.reset_index(inplace=True)
data_dict = df.to_dict("records")

client = pymongo.MongoClient()
db = client['moviedb']
collection = db['movie']

select = ["?film", "?filmLabel", "?id", "?director", "?directorLabel", "?castMember", "?castMemberLabel", "?voiceActor",
          "?voiceActorLabel",
          "?producer", "?producerLabel", "?composer", "?composerLabel", "?mainSubject", "?mainSubjectLabel",
          "?follows", "?followsLabel", "?followedBy", "?followedByLabel", "?publicationDate", "?cost"]
for movie in data_dict:
    select_query = SPARQLSelectQuery(distinct=True)
    # SELECT
    select_query.add_variables(select)
    # Create a graph pattern
    pattern = SPARQLGraphPattern()
    # Add a couple of triples to the pattern
    # WHERE
    pattern.add_triples(
        triples=[
             Triple(subject="?film", predicate="wdt:P3302", object="?id"),
            #Triple(subject="?film", predicate="wdt:P31/wdt:P279*", object="wd:Q11424"),
            #Triple(subject="?film", predicate="rdfs:label", object="?filmLabel"),
            Triple(subject="SERVICE", predicate="wikibase:label", object="{ bd:serviceParam wikibase:language 'en'. }")
        ]
    )

    # Filter
    #pattern.add_filter(filter=Filter(expression="LANG(?filmLabel)='en'"))
    #pattern.add_filter(filter=Filter(expression="REGEX(?filmLabel,'^{0}$')".format(movie['title'])))
    pattern.add_filter(filter=Filter(expression="?id='{0}'".format(movie['id'])))

    for option in [
        {'predicate': "wdt:P57", 'object': "?director"},
        {'predicate': "wdt:P161", 'object': "?castMember"},
        {'predicate': "wdt:P725", 'object': "?voiceActor"},
        {'predicate': "wdt:P162", 'object': "?producer"},
        {'predicate': "wdt:P86", 'object': "?composer"},
        {'predicate': "wdt:P921", 'object': "?mainSubject"},
        {'predicate': "wdt:P155", 'object': "?follows"},
        {'predicate': "wdt:P156", 'object': "?followedBy"},
        {'predicate': "wdt:P577", 'object': "?publicationDate"},
        {'predicate': "wdt:P2130", 'object': "?cost"}]:
        # Create an optional graph pattern
        optionalPattern = SPARQLGraphPattern(optional=True)
        # OPTIONAL
        optionalPattern.add_triples(
            triples=[Triple(subject="?film", predicate=option['predicate'], object=option['object'])])

        # Merge both patterns into one
        pattern.add_nested_graph_pattern(optionalPattern)

    select_query.set_where_pattern(pattern)

    # Let's print this graph pattern
    print(select_query.get_text())

    r = requests.get('https://query.wikidata.org/sparql', params={'query': select_query.get_text()},
                     headers={'accept': 'application/sparql-results+json', 'User-Agent': 'HSD_DAW_BOT/1.0 (https://github.com/Sin-Yone/movie-db-sparql) python-requests'})
    print(r.text)
    print(r.json()['results']['bindings'])
    results = r.json()['results']['bindings']
    if not results: continue

    # Zusammenfassen der Daten
    newMovieData = {
        'director': [],
        'voice_actor': [],
        'producer': [],
        'composer': [],
        'publication_date': results[0].get('publicationDate', {'value': ''})['value'],
        'cost': results[0].get('cost', {'value': ''})['value']
    }

    for result in results:
        if result.get('directorLabel') and result['directorLabel']['value'] not in newMovieData['director']: newMovieData['director'].append(result['directorLabel']['value'])
        if result.get('voiceActorLabel') and result['voiceActorLabel']['value'] not in newMovieData['voice_actor']: newMovieData['voice_actor'].append(result['voiceActorLabel']['value'])
        if result.get('producerLabel') and result['producerLabel']['value'] not in newMovieData['producer']: newMovieData['producer'].append(result['producerLabel']['value'])
        if result.get('composerLabel') and result['composerLabel']['value'] not in newMovieData['composer']: newMovieData['composer'].append(result['composerLabel']['value'])

    print("New Data")
    print(newMovieData)
    # Importieren in DB
    collection.find_one_and_update({'title': results[0]['filmLabel']['value']}, {'$set': newMovieData})
