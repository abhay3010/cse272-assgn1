#Reference : https://radimrehurek.com/gensim/auto_examples/tutorials/run_doc2vec_lee.html
import smart_open
import gensim
from parser_utils import read_documents
from parser_utils import read_queries
from gensim.test.utils import get_tmpfile
from gensim.models.callbacks import CallbackAny2Vec
from gensim.parsing.preprocessing import preprocess_string

class EpochLogger(CallbackAny2Vec):
    def __init__(self):
        super().__init__(self)

        self.epoch = 0
    def on_epoch_begin(self, model):
        print("Epoch #{} start".format(self.epoch), flush=True)
    
    def on_epoch_end(self,model):
        print("Epoch #{} end".format(self.epoch), flush=True)
        self.epoch+=1


def read_corpus_docs(fname):
    for d in read_documents(fname):
        tokens = preprocess_string(d['title']) + preprocess_string(d.get('abstract', ''))
        yield gensim.models.doc2vec.TaggedDocument(tokens, [d['docid']])

def read_corpus_queries(fname):
    for q in read_queries(fname):
        tokens = preprocess_string(q['description'])
        yield gensim.models.doc2vec.TaggedDocument(tokens, [q['num']])
def train_model(inp_filename, save_path):
    
    train_corpus = list(read_corpus_docs(inp_filename))
    print("loaded dataset")
    epoch_logger = EpochLogger()
    model = gensim.models.doc2vec.Doc2Vec(vector_size=512, min_count=4, epochs=40, callbacks=[epoch_logger], workers=6, max_vocab_size=1000000)
    print("building vocab")
    model.build_vocab(train_corpus)
    print("starting training")
    model.train(train_corpus, total_examples=model.corpus_count, epochs=model.epochs)
    print("done training")
    model.save(save_path)

def evaluate_model(model_file, q_file, o_file=None):
    model = gensim.models.doc2vec.Doc2Vec.load(model_file)
    test_dataset = list(read_corpus_queries(q_file))
    with open(o_file, 'w+') as op:
        for d in range(len(test_dataset)):
            inferred_vector = model.infer_vector(test_dataset[d].words)
            sims = model.dv.most_similar([inferred_vector], topn=50)
            for i, (docid, score) in enumerate(sims):
                # print(test_dataset[d].tags[0], docid, score)

                op.write('{0} \tQ0\t{1}\t{2}\t{3}\t{4}\n'.format(test_dataset[d].tags[0], docid, i+1, score, 'doc2vev' ))
def main2():
    ds_file = '/Users/abhaypande/personal/UCSC/spring21/retrieval/assignment1_submission/python-assignment1/cse272-assignment1/data/ohsumed.88-91'
    train_model(ds_file, 'doc2vec_small')


def main():
    ds_file = '/Users/abhaypande/personal/UCSC/spring21/retrieval/assignment1_submission/python-assignment1/cse272-assignment1/data/ohsumed.88-91'
    q_file = '/Users/abhaypande/personal/UCSC/spring21/retrieval/assignment1_submission/python-assignment1/cse272-assignment1/data/query.ohsu.1-63'
    evaluate_model('doc2vec_smaller', q_file, 'doc2vec.txt')

if __name__ == '__main__':
    main()



    