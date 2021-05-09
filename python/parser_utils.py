from whoosh.index import create_in
import whoosh.index as index
from whoosh.fields import *
from whoosh.qparser import QueryParser
from whoosh.filedb.filestore import FileStorage
from whoosh.filedb.filestore import RamStorage
from whoosh.analysis import StemmingAnalyzer
from whoosh.writing import AsyncWriter
from whoosh.qparser import MultifieldParser
from whoosh import qparser
from nltk.corpus import stopwords
import whoosh.scoring as scoring

TITLE = "title"
DESC = "description"
NUM = "num"
D_SID = '.I'
D_UI = '.U'
F_UID = "docid"
D_MESH = '.M'
F_MESH = "mesh_terms"
D_TITLE = '.T'
F_TITLE = "title"
D_PUB = '.P'
F_PUB = "publication"
D_ABS = '.W'
F_ABS = "abstract"
D_SOURCE = '.S'
F_SOURCE = "source"
D_AUTHOR = '.A'
F_AUTHOR = "author"


def read_queries(filename):
    queries = list()
    with open(filename, 'r+') as f:
        current_query = None
        for line in f:
            line = line[:-1]
            if '<top>' in line:
                current_query = dict()
            elif '</top>' in line:
                queries.append(current_query)
                current_query = dict()
            elif '<num>' in line:
                current_query[NUM] = line.split(':')[1].strip()
            elif '<title>' in line:
                current_query[TITLE] = line.split('>')[1].strip()
            elif (not '<desc>' in line and len(line) > 2):
                current_query[DESC] = line
    return queries
def read_documents(filename):
    current_document = None
    current_field = None
    with open(filename, 'r+') as f:
        for line in f:
            line = line[:-1]
            if line.startswith(D_SID):
                if current_document is not None:
                    yield current_document
                current_document = dict()
            elif line.startswith(D_UI):
                current_field = F_UID
            elif line.startswith(D_SOURCE):
                current_field = F_SOURCE
            elif line.startswith(D_MESH):
                current_field = F_MESH
            elif line.startswith(D_TITLE):
                current_field = F_TITLE
            elif line.startswith(D_PUB):
                current_field = F_PUB
            elif line.startswith(D_ABS):
                current_field = F_ABS
            elif line.startswith(D_AUTHOR):
                current_field = F_AUTHOR
            else:
                current_document[current_field] = line
    yield current_document
    return
def create_index(fname, index_name):
    filtered_fields = [F_TITLE, F_ABS, F_MESH, F_PUB, F_AUTHOR,F_UID  ]
    # analyzer=StemmingAnalyzer()
    # analyzer.cachesize=-1
    schema = Schema(title=TEXT(stored=False, phrase=False), 
    abstract=TEXT(stored=False, phrase=True, analyzer=StemmingAnalyzer(stoplist=stopwords.words('english'))), mesh_terms=KEYWORD(stored=False), 
    publication = KEYWORD(stored=False), 
    author = KEYWORD(stored=False), 
    docid = ID(stored=True)
    )
    storage = FileStorage(index_name).create()
    ix = storage.create_index( schema)
    #writer = ix.writer(limitmb=1024, procs=8)
    
    writer = AsyncWriter(ix, delay=0.25, writerargs={'limitmb':1024, 'procs':8, 'segment':True})

    for i, d in enumerate(read_documents(fname)):
        writer.add_document(**{k:v for k,v in d.items() if k in filtered_fields})
        if (i+1)%50000 == 0:
            print(i)
    writer.commit()
    storage.close()
    print(("commit done"))
    return ix
def run_experiments(index_dir, qfile, outputfile='evaluation.txt', run='boolean'):
    queries = read_queries(qfile)
    ix = index.open_dir(index_dir)
    qp = MultifieldParser([F_ABS,F_TITLE], schema=ix.schema, group=qparser.OrGroup)
    with ix.searcher(weighting=scoring.TF_IDF()) as s:
        with open(outputfile,'w+') as output:
            for i, q in enumerate(queries):
                query = qp.parse(q['description'])
                results = s.search(query, limit=50)
                for rank, result in enumerate(results):
                    output.write('{0} \tQ0\t{1}\t{2}\t{3}\t{4}\n'.format(q[NUM], result['docid'], result.rank, result.score, run))



def main():
    fname = "/Users/abhaypande/personal/UCSC/spring21/retrieval/assignment1_submission/python-assignment1/cse272-assignment1/data/ohsumed.88-91"
    qfname = "/Users/abhaypande/personal/UCSC/spring21/retrieval/assignment1_submission/python-assignment1/cse272-assignment1/data/query.ohsu.1-63"
    # run_experiments("index2", qfname)
    create_index(fname, "index_abspos_nostop")
def main2():
     qfname = "/Users/abhaypande/personal/UCSC/spring21/retrieval/assignment1_submission/python-assignment1/cse272-assignment1/data/query.ohsu.1-63"
     run_experiments("index_abspos_nostop", qfname, outputfile='evaluation_v2.txt', run='boolean-without-stop')

if __name__ == '__main__':
    main2()










