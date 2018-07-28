package com.capgemini.migrator;

import java.io.*;
import java.util.*;

import com.sforce.soap.enterprise.sobject.Solution;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.prefs.CsvPreference;

public class VMwareDataParser {

    static final String[] COLUMN_HEADER = new String[]{"Account", "Field", "Response", "Siding", "Roofing"};
    static final String csvToCompareColumns = "/Users/alanbrown/Downloads/TapDataLoad.csv";
    static final String RESULT_FILE = "/Users/alanbrown/Downloads/TAPLoadableData.csv";


    //DataCenter, Public Cloud, Digital Workspace, Security
    //Process Solution Area Name to see which of the 4 we're processing
    //PartnerId maps to field API Name
    //Response is field value
    //Solution Area Name maps to multiple Name fields

    public static void main(String[] args) throws Exception {

        File resultFile = new File(RESULT_FILE);

        VMwareDataParser parser = new VMwareDataParser();
        List<SolutionArea> solutionAreas = parser.getSolutionAreas(csvToCompareColumns);
        parser.writeResults(resultFile, solutionAreas);
    }

    private List<SolutionArea> getSolutionAreas(String fileName) throws IOException {

        CsvReadingInfo readingInfo = getIdMapperReader(fileName);
        List<SolutionArea> result = new ArrayList<>();
        String[] nameMapping = readingInfo.getNameMapping();
        CsvMapReader reader = readingInfo.getReader();

        Map<String, String> currLine;
        String questionnaire = "";
        SolutionArea nextSolutionarea = new SolutionArea();
        result.add(nextSolutionarea);


        //DataCenter, Public Cloud, Digital Workspace, Security
        String[] names = {"Data Center", "Public Cloud", "Digital Workspace", "Security"};
        int nameCount = -1;

        while ((currLine = reader.read(nameMapping)) != null) {

            if (currLine.get("Field").equals("Area_Of_Interest__c")) {
                nextSolutionarea = new SolutionArea();
                result.add(nextSolutionarea);
                nextSolutionarea.Area_Of_Interest__c = currLine.get("Response");
                nameCount++;
            } else {
                switch (currLine.get("Field")) {
                    case "Company_Technical_Objective__c":
                        nextSolutionarea.Company_Technical_Objective__c = currLine.get("Response");
                        break;
                    case "Technical_Contact_Name__c":
                        nextSolutionarea.Technical_Contact_Name__c = currLine.get("Response");
                        break;
                    case "Technical_Contact_Phone__c":
                        nextSolutionarea.Technical_Contact_Phone__c = currLine.get("Response");
                        break;
                    case "Technical_Contact_Email__c":
                        nextSolutionarea.Technical_Contact_Email__c = currLine.get("Response");
                        break;
                    case "Technical_Contact_Job_Title__c":
                        nextSolutionarea.Technical_Contact_Job_Title__c = currLine.get("Response");
                        break;
                    case "Other_Cloud_Platforms__c":
                        nextSolutionarea.Other_Cloud_Platforms__c = currLine.get("Response");
                        break;
                    default:

                }

                nextSolutionarea.AccountId = currLine.get("AccountId");
                if (nameCount >= 0) {
                    nextSolutionarea.name = names[nameCount % 4];
                    nextSolutionarea.Solution_Area_Name__c = names[nameCount % 4];
                }
            }



        }


        return result;
    }

    private static void writeResults(File resultFile, List<SolutionArea> solutionAreas)  {
        FileWriter fw = null;
        CsvMapWriter writer = null;
        try {
            fw = new FileWriter(resultFile);
            writer = new CsvMapWriter(fw, CsvPreference.EXCEL_PREFERENCE);

            String[] outputHeader = {"AccountId", "Name", "Solution_Area_Name__c", "Area_Of_Interest__c", "Company_Technical_Objective__c",
                    "Technical_Contact_Name__c", "Technical_Contact_Phone__c", "Technical_Contact_Email__c", "Technical_Contact_Job_Title__c",
                    "Other_Cloud_Platforms__c"};

            writer.writeHeader(outputHeader);

            for (SolutionArea nextSolutionArea : solutionAreas) {
                Map<String, String> currRecord = new HashMap<String, String>();
                currRecord.put("Name", nextSolutionArea.name);
                currRecord.put("Solution_Area_Name__c", nextSolutionArea.Solution_Area_Name__c);
                currRecord.put("Area_Of_Interest__c", nextSolutionArea.Area_Of_Interest__c);
                currRecord.put("Company_Technical_Objective__c", nextSolutionArea.Company_Technical_Objective__c);
                currRecord.put("Technical_Contact_Name__c", nextSolutionArea.Technical_Contact_Name__c);
                currRecord.put("Technical_Contact_Phone__c", nextSolutionArea.Technical_Contact_Phone__c);
                currRecord.put("Technical_Contact_Email__c", nextSolutionArea.Technical_Contact_Email__c);
                currRecord.put("Technical_Contact_Job_Title__c", nextSolutionArea.Technical_Contact_Job_Title__c);
                currRecord.put("Other_Cloud_Platforms__c", nextSolutionArea.Other_Cloud_Platforms__c);
                currRecord.put("AccountId", nextSolutionArea.AccountId);
                if (nextSolutionArea.Area_Of_Interest__c != null && nextSolutionArea.Area_Of_Interest__c.length() > 0)
                    writer.write(currRecord, outputHeader);
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

    class SolutionArea {
        String name = "";
        String Solution_Area_Name__c = "";
        String Area_Of_Interest__c = "";
        String Company_Technical_Objective__c = "";
        String Technical_Contact_Name__c = "";
        String Technical_Contact_Phone__c = "";
        String Technical_Contact_Email__c = "";
        String Technical_Contact_Job_Title__c = "";
        String Other_Cloud_Platforms__c = "";
        String AccountId = "";
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




}
