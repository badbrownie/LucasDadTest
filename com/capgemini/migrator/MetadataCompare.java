package com.capgemini.migrator;

import java.io.*;
import java.util.*;

import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.SaveResult;
import com.sforce.soap.enterprise.sobject.Attachment;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.prefs.CsvPreference;

public class MetadataCompare {

    static final String[] COLUMN_HEADER = new String[]{"Insulation", "Ceiling", "Gypsum", "Siding", "Roofing"};
    static final String csvToCompareColumns = "/Users/alanbrown/Downloads/CaseFileldsToCompare.csv";
    static final String RESULT_FILE = "/Users/alanbrown/Downloads/CaseFieldCompare.csv";

    public static void main(String[] args) throws Exception {

        File resultFile = new File(RESULT_FILE);

        MetadataCompare comparer = new MetadataCompare();
        Map<String, LinkedList<String>> listsToCompare = comparer.getListsOfTexts(csvToCompareColumns);
        comparer.writeResults(resultFile, listsToCompare);




    }

    private static String getNextString(Map<String, LinkedList<String>> listsToCompare) {
        String result = null;

        for (int i = 0; i < COLUMN_HEADER.length; i++) {
            if (!listsToCompare.get(COLUMN_HEADER[i]).isEmpty()) {
                if (result == null && listsToCompare.get(COLUMN_HEADER[i]).element() != null)
                    result = listsToCompare.get(COLUMN_HEADER[i]).element().toLowerCase();
                else if (listsToCompare.get(COLUMN_HEADER[i]).element() != null
                        && result.compareTo(listsToCompare.get(COLUMN_HEADER[i]).element().toLowerCase()) > 0)
                    result = listsToCompare.get(COLUMN_HEADER[i]).element().toLowerCase();
            }
        }
        return result;
    }

    private static void writeResults(File resultFile, Map<String, LinkedList<String>> listsToCompare)  {
        FileWriter fw = null;
        CsvMapWriter writer = null;
        try {
            fw = new FileWriter(resultFile);
            writer = new CsvMapWriter(fw, CsvPreference.EXCEL_PREFERENCE);

            String[] outputHeader = new String[COLUMN_HEADER.length+1];
            for (int i=0; i < COLUMN_HEADER.length; i++) {
                outputHeader[i+1] = COLUMN_HEADER[i];
            }
            outputHeader[0] = "Count";
            writer.writeHeader(outputHeader);

            String nextString = getNextString(listsToCompare);


            while (nextString != null && nextString.length() > 0) {
                Map<String, String> currRecord = new HashMap<String, String>();
                int count = 0;

                for (int i = 0; i < COLUMN_HEADER.length; i++) {
                    if (!listsToCompare.get(COLUMN_HEADER[i]).isEmpty()
                            && listsToCompare.get(COLUMN_HEADER[i]).element() != null
                            && listsToCompare.get(COLUMN_HEADER[i]).element().equalsIgnoreCase(nextString)) {
                        currRecord.put(COLUMN_HEADER[i], listsToCompare.get(COLUMN_HEADER[i]).removeFirst());
                        count++;
                    }
                }
                currRecord.put("Count",  "" + count);
                writer.write(currRecord, outputHeader);
                nextString = getNextString(listsToCompare);
            }


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null)
                    writer.close();
                if (fw != null)
                    fw.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

        }


    }




    private Map<String, LinkedList<String>> getListsOfTexts(String fileName) throws IOException {

        CsvReadingInfo readingInfo = getIdMapperReader(fileName);
        Map<String, LinkedList<String>> result = new HashMap<>();

        String[] nameMapping = readingInfo.getNameMapping();
        CsvMapReader reader = readingInfo.getReader();

        Map<String, String> currLine;
        while ((currLine = reader.read(nameMapping)) != null) {
            for (int i = 0; i < COLUMN_HEADER.length; i++) {
                if (result.get(COLUMN_HEADER[i]) == null)
                    result.put(COLUMN_HEADER[i], new LinkedList<>());
                if (currLine.get(COLUMN_HEADER[i]) != null)
                    result.get(COLUMN_HEADER[i]).add(currLine.get(COLUMN_HEADER[i]));
            }

        }
        for (int i = 0; i < COLUMN_HEADER.length; i++) {
            Collections.sort(result.get(COLUMN_HEADER[i]), String.CASE_INSENSITIVE_ORDER);

        }


        return result;
    }

    private CsvReadingInfo getIdMapperReader(String fileName) throws IOException {
        File file = new File(fileName);
        System.out.println(file.getAbsolutePath());
        if (!file.exists()) {
            throw new FileNotFoundException("No such file: " + fileName);
        }
        List<String> lines = null;
        BufferedReader in = new BufferedReader(new FileReader(file));

        CsvMapReader reader = new CsvMapReader(in, CsvPreference.STANDARD_PREFERENCE);
        String[] nameMapping = reader.getHeader(true);
        return new CsvReadingInfo(reader, nameMapping);
    }





    class CsvReadingInfo {
        public CsvMapReader reader;
        public String[] nameMapping;

        CsvReadingInfo(CsvMapReader reader, String[] nameMapping) {
            this.reader = reader;
            this.nameMapping = nameMapping;
        }

        public CsvMapReader getReader() {
            return reader;
        }

        public String[] getNameMapping() {
            return nameMapping;
        }
    }


    class IdMapper {
        public String legacyId;
        public String newId;

        IdMapper(String legacyId, String newId) {
            this.legacyId = legacyId;
            this.newId = newId;
        }

        public String getLegacyId() {
            return legacyId;
        }

        public String getNewId() {
            return newId;
        }
    }


}
