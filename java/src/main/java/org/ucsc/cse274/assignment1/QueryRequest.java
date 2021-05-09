package org.ucsc.cse274.assignment1;

import lombok.Data;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Data
public class QueryRequest {
    String number;
    String title;
    String description;

    public static List<QueryRequest> getQueryRequestsFromFile(String filename) throws IOException{
        List<QueryRequest> queryLines = new ArrayList<QueryRequest>();
        QueryRequest currentQuery = null;
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filename))) {
            String line=  br.readLine();
            String previousTag = null;
            while (line != null) {
                System.out.println(line);
                if (line.startsWith("</top>")){
                    if (currentQuery != null){
                        queryLines.add(currentQuery);
                    }

                }
                else if (line.startsWith("<top>")) {
                    currentQuery = new QueryRequest();
                }
                else if (line.startsWith("<num>")){
                    String number =line.split(":")[1].trim();
                    currentQuery.setNumber(number);

                }
                else if (line.startsWith("<title>")){
                    String title = line.split(">")[1].trim();
                    currentQuery.setTitle(title);
                }
                else if (!line.startsWith("<desc>") && line.length() > 2) {
                    currentQuery.setDescription(line);
                }
                line = br.readLine();
            }


        }
        System.out.println(queryLines);
        return queryLines;
    }

}
