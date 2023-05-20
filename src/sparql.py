from SPARQLBurger.SPARQLQueryBuilder import *
import requests
import pandas as pd
import pymongo

# How many movies should be processed at once
FETCH_AMOUNT = 25
file = 'movies_metadata.csv'
df = pd.read_csv(file)
df.reset_index(inplace=True)
movieDataDict = df.to_dict("records")
client = pymongo.MongoClient()
db = client['moviedb']
collection = db['movie']
ids = list(map(lambda doc: doc['imdb_id'], movieDataDict))
idCount = len(ids)
select = ["?film", "?filmLabel", "?id", "?director", "?directorLabel", "?castMember", "?castMemberLabel", "?voiceActor",
          "?voiceActorLabel", "?producer", "?producerLabel", "?composer", "?composerLabel", "?mainSubject",
          "?mainSubjectLabel", "?follows", "?followsLabel", "?followedBy", "?followedByLabel", "?publicationDate",
          "?cost"]
fetchedElements = 0

while fetchedElements < idCount:
    currentIds = ids[fetchedElements:fetchedElements + FETCH_AMOUNT]
    select_query = SPARQLSelectQuery(distinct=True)
    # SELECT
    select_query.add_variables(select)
    # Create a graph pattern
    pattern = SPARQLGraphPattern()

    # WHERE
    pattern.add_triples(
        triples=[
            Triple(subject="?film", predicate="wdt:P345", object="?id"),
            Triple(subject="SERVICE", predicate="wikibase:label", object="{ bd:serviceParam wikibase:language 'en'. }")
        ]
    )

    # Filter
    pattern.add_filter(filter=Filter(expression="?id IN({0})".format(', '.join(f'"{_id}"' for _id in currentIds))))

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
    r = requests.get('https://query.wikidata.org/sparql', params={'query': select_query.get_text()},
                     headers={'accept': 'application/sparql-results+json',
                              'User-Agent': 'HSD_DAW_BOT/1.0 (https://github.com/Sin-Yone/movie-db-sparql) python-requests'})
    results = r.json()['results']['bindings']
    sortedMovies = {}
    for movieData in results:
        if not movieData: continue
        if movieData['id']['value'] not in sortedMovies:
            sortedMovies[movieData['id']['value']] = [movieData]
        else:
            sortedMovies[movieData['id']['value']].append(movieData)
    for movieId, movies in sortedMovies.items():
        # merge all movie data into one object
        newMovieData = {
            'director': [],
            'cast_member': [],
            'voice_actor': [],
            'producer': [],
            'composer': [],
            'main_subject': [],
            'publication_date': movies[0].get('publicationDate', {'value': ''})['value'],
            'cost': movies[0].get('cost', {'value': ''})['value'],
            'follows': movies[0].get('followsLabel', {'value': ''})['value'],
            'followed_by': movies[0].get('followedByLabel', {'value': ''})['value'],
        }

        for movie in movies:
            if movie.get('directorLabel') and movie['directorLabel']['value'] not in newMovieData['director']:
                newMovieData[
                    'director'].append(movie['directorLabel']['value'])
            if movie.get('castMemberLabel') and movie['castMemberLabel']['value'] not in newMovieData['cast_member']:
                newMovieData[
                    'cast_member'].append(movie['castMemberLabel']['value'])
            if movie.get('voiceActorLabel') and movie['voiceActorLabel']['value'] not in newMovieData['voice_actor']:
                newMovieData[
                    'voice_actor'].append(movie['voiceActorLabel']['value'])
            if movie.get('producerLabel') and movie['producerLabel']['value'] not in newMovieData['producer']:
                newMovieData[
                    'producer'].append(movie['producerLabel']['value'])
            if movie.get('composerLabel') and movie['composerLabel']['value'] not in newMovieData['composer']:
                newMovieData[
                    'composer'].append(movie['composerLabel']['value'])
            if movie.get('mainSubjectLabel') and movie['mainSubjectLabel']['value'] not in newMovieData['main_subject']:
                newMovieData[
                    'main_subject'].append(movie['mainSubjectLabel']['value'])

        # Update db with new data
        collection.update_many({'imdb_id': movieId}, {'$set': newMovieData})
    fetchedElements += len(currentIds)
    print('Movies updated: {0} ({1}%)'.format(fetchedElements, round((fetchedElements * 100) / len(ids), 2)))
