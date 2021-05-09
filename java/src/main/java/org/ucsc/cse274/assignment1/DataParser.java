package org.ucsc.cse274.assignment1;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.util.QueryBuilder;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.rmi.server.ExportException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataParser {
    static String SID = ".I";
    static String UI = ".U";
    static String UI_FIELD = "universal_identifier";
    static String MESH = ".M";
    static String MESH_FIELD = "keywords";
    static String TITLE = ".T";
    static String TITLE_FIELD = "title";

    static String PUBLICATION = ".P";
    static String PUB_FIELD = "publication";
    static String ABSTRACT = ".W";
    static String ABS_FIELD = "abstract";
    static String SOURCE = ".S";
    static String SOURCE_FIELD = "source";

    public static RAMDirectory buildIndex() throws IOException {
        String resourceFile = "src/main/resources/ohsumed.88-91";
        RAMDirectory memoryIndex = new RAMDirectory();
        EnglishAnalyzer analyzer = new EnglishAnalyzer();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        BufferedReader reader;
        IndexWriter writer = null;
        try {
            writer = new IndexWriter(memoryIndex, indexWriterConfig);

            reader = new BufferedReader(new FileReader(resourceFile));
            String line = reader.readLine();
            int count = 0;
            Document currentDocument = null;
            String currentField = null;
            Field.Store store = Field.Store.NO;
            while (line != null) {
                count += 1;
                if (line.startsWith(SID)) {
                    if (currentDocument != null) {
                        writer.addDocument(currentDocument);
                    }
                    currentDocument = new Document();

                } else if (line.startsWith(UI)) {
                    currentField = UI_FIELD;
                    store = Field.Store.YES;
                } else if (line.startsWith(MESH)) {
                    currentField = MESH_FIELD;
                    store = Field.Store.NO;

                } else if (line.startsWith(TITLE)) {
                    currentField = TITLE_FIELD;
                    store = Field.Store.NO;

                } else if (line.startsWith(PUBLICATION)) {
                    currentField = PUB_FIELD;
                    store = Field.Store.NO;


                } else if (line.startsWith(ABSTRACT)) {
                    currentField = ABS_FIELD;
                    store = Field.Store.NO;

                } else if (line.startsWith(SOURCE)) {
                    currentField = SOURCE_FIELD;
                    store = Field.Store.NO;

                } else {
                    currentDocument.add(new TextField(currentField, line, store));
                }
                line = reader.readLine();

            }
            writer.addDocument(currentDocument);
            writer.commit();


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null && writer.isOpen()) {
                try {
                    writer.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return memoryIndex;


    }

    public DataParser() {
    }

    public static void main(String args[]) throws IOException, ParseException {
        String queryFile = "src/main/resources/query.ohsu.1-63";
        String outputfile = "src/main/resources/boosts.txt";
        Directory index = buildIndex();
        DirectoryReader directoryReader = DirectoryReader.open(index);
        System.out.println(directoryReader.numDocs());
        final IndexSearcher searcher = new IndexSearcher(directoryReader);
//        searcher.setSimilarity(new ClassicSimilarity());
        final Analyzer analyzer = new EnglishAnalyzer();
        Map<String, Float> boosts = new HashMap<String, Float>();
        boosts.put("title", 1.3f);
        boosts.put("source", 0.5f);
        boosts.put("abstract", 0.9f);
        boosts.put("publication", 0.5f);
        boosts.put("keywords", 1.3f);
        QueryParser queryParser = new MultiFieldQueryParser(new String[]{"title", "source", "abstract", "publication", "keywords"}, analyzer, boosts);

        List<QueryRequest> queries = QueryRequest.getQueryRequestsFromFile(queryFile);
        int count = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputfile), StandardOpenOption.WRITE)) {
            for (QueryRequest query : queries) {
                try {
                    Query q1 = queryParser.parse(query.title);
                    Query q2 = queryParser.parse(query.description);
                    BooleanQuery bq = new BooleanQuery.Builder()
                            .add(q1, BooleanClause.Occur.SHOULD)
                            .add(q2, BooleanClause.Occur.SHOULD)
                            .build();
                    TopDocs topDocs = searcher.search(q2, 50);
                    final List<Document> docs = new ArrayList<>();
                    count = 1;
                    for (final ScoreDoc scoreDoc : topDocs.scoreDocs) {
                        Document d = searcher.doc(scoreDoc.doc);
                        writer.write(MessageFormat.format("{0}\tQ0\t{1}\t{2}\t{3}\t{4}\n",
                                query.getNumber(), searcher.doc(scoreDoc.doc).getField(UI_FIELD).stringValue(),
                                count, scoreDoc.score, "bm25-boosts"));

                        count+=1;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }


            }
        } finally {
        }

    }
}
