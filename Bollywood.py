from arango import ArangoClient

client = ArangoClient()

sys_db = client.db('_system',username='root',password='passwrd') 
# Change the password to your corresponding root password.

if not sys_db.has_database('Bollywood'):
    sys_db.create_database('Bollywood')

db = client.db('Bollywood',username='root',password='passwrd')
# Change the password to your corresponding root password.

# Starting to create Graph for database
if not db.has_graph('Graph'):
    db.create_graph('Graph')

Graph = db.graph('Graph')

# The required graph will have three vertex-collections : Actors , Movies , Directors.

#Starting Actors collection and it's data input.
if not Graph.has_vertex_collection('Actors'):
    Graph.create_vertex_collection('Actors')

Actors = Graph.vertex_collection('Actors')
Actors.truncate()

Actors.insert({'_key':'varun','name':'Varun'})
Actors.insert({'_key':'srk','name':'SRK'})
Actors.insert({'_key':'alia','name':'Alia'})

#Starting Movies collection and it's data input.

if not Graph.has_vertex_collection('Movies'):
    Graph.create_vertex_collection('Movies')

Movies = Graph.vertex_collection('Movies')
Movies.truncate()

Movies.insert({'_key':'soty','movie':'SOTY'})
Movies.insert({'_key':'bkd','movie':'BKD'})
Movies.insert({'_key':'dilwale','movie':'Dilwale'})
Movies.insert({'_key':'zero','movie':'Zero'})
Movies.insert({'_key':'dz','movie':'DZ'})

#Starting Directors collection and it's data input.

if not Graph.has_vertex_collection('Directors'):
    Graph.create_vertex_collection('Directors')

Directors = Graph.vertex_collection('Directors')

Directors.truncate()


Directors.insert({'_key':'karan','director':'Karan'})
Directors.insert({'_key':'shashank','director':'Shashank'})
Directors.insert({'_key':'rohit','director':'Rohit'})
Directors.insert({'_key':'anand','director':'Anand'})
Directors.insert({'_key':'gauri','director':'Gauri'})


# Starting of Edge-definition for Graph for one of the three different edge collections.

# Code For Type1 edge-collection for edge 
# between actors(from) and movies(to)--

if not Graph.has_edge_definition('Type1'):
    Graph.create_edge_definition(
        edge_collection='Type1',
        from_vertex_collections=['Actors'],
        to_vertex_collections=['Movies']
    )

Type1 = Graph.edge_collection('Type1')


Type1.truncate()

Type1.insert({'_key':'varun-soty','_from':'Actors/varun','_to':'Movies/soty'})
Type1.insert({'_key':'varun-bkd','_from':'Actors/varun','_to':'Movies/bkd'})
Type1.insert({'_key':'varun-dilwale','_from':'Actors/varun','_to':'Movies/dilwale'})
Type1.insert({'_key':'srk-dilwale','_from':'Actors/srk','_to':'Movies/dilwale'})
Type1.insert({'_key':'srk-zero','_from':'Actors/srk','_to':'Movies/zero'})
Type1.insert({'_key':'srk-dz','_from':'Actors/srk','_to':'Movies/dz'})
Type1.insert({'_key':'alia-soty','_from':'Actors/alia','_to':'Movies/soty'})
Type1.insert({'_key':'alia-bkd','_from':'Actors/alia','_to':'Movies/bkd'})
Type1.insert({'_key':'alia-dz','_from':'Actors/alia','_to':'Movies/dz'})

# Code For Type2 edge-collection for edge between 
# directors(from) and movies(to) --


if not Graph.has_edge_definition('Type2'):
    Graph.create_edge_definition(
        edge_collection='Type2',
        from_vertex_collections=['Directors'],
        to_vertex_collections=['Movies']
    )

Type2 = Graph.edge_collection('Type2')

Type2.truncate()

Type2.insert({'_key':'karan-soty','_from':'Directors/karan','_to':'Movies/soty'})
Type2.insert({'_key':'shashank-bkd','_from':'Directors/shashank','_to':'Movies/bkd'})
Type2.insert({'_key':'rohit-dilwale','_from':'Directors/rohit','_to':'Movies/dilwale'})
Type2.insert({'_key':'anand-zero','_from':'Directors/anand','_to':'Movies/zero'})
Type2.insert({'_key':'gauri-dz','_from':'Directors/gauri','_to':'Movies/dz'})

# Code For Type3 edge-collection for edge between 
# movies(from) and directors(to) --

if not Graph.has_edge_definition('Type3'):
    Graph.create_edge_definition(
        edge_collection='Type3',
        to_vertex_collections=['Directors'],
        from_vertex_collections=['Movies']
    )

Type3 = Graph.edge_collection('Type3')

Type3.truncate()

Type3.insert({'_key':'karan-soty','_to':'Directors/karan','_from':'Movies/soty'})
Type3.insert({'_key':'shashank-bkd','_to':'Directors/shashank','_from':'Movies/bkd'})
Type3.insert({'_key':'rohit-dilwale','_to':'Directors/rohit','_from':'Movies/dilwale'})
Type3.insert({'_key':'anand-zero','_to':'Directors/anand','_from':'Movies/zero'})
Type3.insert({'_key':'gauri-dz','_to':'Directors/gauri','_from':'Movies/dz'})

# Now the functions for queries is starting

# This function returns the movies list of given Actor 
# (passed as parameter to function) that are stored in 
# the database :-
def Actor_Movie_list(actor):
    s = actor.lower()
    if not Actors.has(s):
        return []

    vertex_list = Graph.traverse(
        start_vertex='Actors/'+s,
        direction='outbound',
        strategy='bfs',
        max_depth='1'
    )['vertices']

    list=[]
    for i in range(len(vertex_list)):
        if i!=0:
            list.append(vertex_list[i]['movie'])
    
    return list
# Test the above function by uncommenting the next line. 
# print(Actor_Movie_list('aLIA'))


# This function returns the common movie list of two Actors
# (passed as parameter) from the database
def Movie_list_of_2_actor(A1,A2):
    L1 = Actor_Movie_list(A1)
    L2 = Actor_Movie_list(A2)

    if L1==[] or L2==[]:
        print ('No common movie for both actor.')
        return []

    else:
        set1 = set(L1)
        set2 = set(L2)

        common_movie = set1.intersection(set2)
        common_movie_list = list(common_movie)

        if common_movie_list is []:
            print('No common movie for both actor.')
            return []
        else:
            return common_movie_list

# Test the above function by uncommenting the next line. 
# print(Movie_list_of_2_actor('Varun','alia'))


# This function returns the movies list of given Director 
# (passed as parameter to function) that are stored in 
# the database :-
def Director_Movie_list(D):
    s = D.lower()
    if not Directors.has(s):
        return []

    vertex_list = Graph.traverse(
        start_vertex='Directors/'+s,
        direction='outbound',
        strategy='bfs',
        max_depth='1'
    )['vertices']

    list=[]
    for i in range(len(vertex_list)):
        if i!=0:
            list.append(vertex_list[i]['movie'])
    
    return list


# This function returns the common movie list of an actor and a director
# (passed as parameter) from the database
def Movie_of_Actor_Direc(A,D):
    L1 = Actor_Movie_list(A)
    L2 = Director_Movie_list(D)

    if L1==[] or L2==[]:
        print ('No common movie .')
        return []

    else:
        set1 = set(L1)
        set2 = set(L2)

        common_movie = set1.intersection(set2)
        common_movie_list = list(common_movie)

        if common_movie_list is []:
            print('No common movie .')
            return []
        else:
            return common_movie_list

# Test the above function by uncommenting the next line. 
# print(Movie_of_Actor_Direc('alia','shashank'))


# This function return the list of directors for an Actor
# (passed as a parameter to the function) that are stored 
# in the database
def Director_for_Actor(A):
    s = A.lower()
    if not Actors.has(s):
        return []
    
    vertex_list = Graph.traverse(
        start_vertex='Actors/'+s,
        direction='outbound',
        strategy='bfs',
        max_depth='2'
    )['vertices']

    list=[]
    LENGTH = len(vertex_list)
    for i in range(LENGTH):
        if 2*i > LENGTH:
            list.append(vertex_list[i]['director'])
    
    return list
# Test the above function by uncommenting the next line. 
# print(Director_for_Actor('srk'))