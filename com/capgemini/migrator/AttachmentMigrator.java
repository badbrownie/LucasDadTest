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

public class AttachmentMigrator {

    //static final String EXTRACT_FILE_LOCATION = "../../leadExtracts.csv";
    static final String ATTACHMENT_ID_MAP_FILE = "/Users/alanbrown/Downloads/ParentidLittle.csv";
    public static final String ENDPOINT = "https://login.salesforce.com/services/Soap/c/40.0";
    //public static final String ENDPOINT = "https://test.salesforce.com/services/Soap/c/40.0";
    //static final String OWNER_ID_MAP_FILE = "/Users/alanbrown/Documents/ownerIdMap.csv";

    static final String OWNER_ID_MAP_FILE = "";
    static final int BIG_IN_SIZE = 90;

    static int OneOffDeleteMe = 0;

    static final String[] ATTACHMENT_HEADER = new String[]{"ParentId", "Name", "ContentType", "isPrivate", "owner"};
    static final String[] SUCCESS_HEADER = new String[]{"id", "ParentId", "Name", "ContentType", "isPrivate", "owner"};
    static final String[] FAILURE_HEADER = new String[]{"id", "ParentId", "Name", "ContentType", "isPrivate", "owner", "ERROR"};

    //static final String[] ATTACHMENT_HEADER = new String[]{"ParentId", "Name", "isPrivate", "owner"};
    //static final String[] SUCCESS_HEADER = new String[]{"id", "ParentId", "Name", "isPrivate", "owner"};
    //static final String[] FAILURE_HEADER = new String[]{"id", "ParentId", "Name", "isPrivate", "owner", "ERROR"};


    static final String ATTACHMENT_EXTRACT = "/Users/alanbrown/Documents/Flex10_1ToNow.csv";


    static final boolean FILE_HAS_HEADER_ROW = true;
    static final String TARGET_DIR = "javaExport10";

    static final int BATCH_SIZE = 10;


    static final String SRC_USERNAME = "alan.a.brown@capgemini.com";
    static final String SRC_PASSWORD = "qwerqwer3#HUbQ0zMZ79rfbssRU9WjLraw";

    static final String TAGET_USERNAME = "alan.a.brown@te.com.c2s.volumetest";
    static final String TARGET_PASSWORD = "qwerqwer2";
    static EnterpriseConnection TARGET_connection;

    static List<String> AttachmentIds = new ArrayList<String>();


    static final boolean EXTRACT = false;
    static boolean initialized = false;


    public static void main(String[] args) throws Exception {

        File file = new File(TARGET_DIR);
        file.mkdir();

        AttachmentMigrator migrator = new AttachmentMigrator();

        Map<String, String> attachmentIdMap = migrator.getLegacyToNewIdMap(ATTACHMENT_ID_MAP_FILE);
        Map<String, String> reverseAttachmentIdMap = new HashMap<String, String>();

        for (String s : attachmentIdMap.keySet()) {
            System.out.println("key/Value is " + s + "/" + attachmentIdMap.get(s));
            reverseAttachmentIdMap.put(attachmentIdMap.get(s), s);
        }

        Map<String, String> ownerIdMap = null;

        if (new File(OWNER_ID_MAP_FILE).exists())    {
            ownerIdMap = migrator.getLegacyToNewIdMap(OWNER_ID_MAP_FILE);

            for (String s : ownerIdMap.keySet()) {
                System.out.println("key/Value is " + s + "/" + ownerIdMap.get(s));
            }
        }


        migrator.extractAttachments(attachmentIdMap, ownerIdMap);

        File alanLoaderResult = new File("/Users/alanbrown/Documents/alanLoaderResults");
        alanLoaderResult.mkdirs();

        Date now = new Date();
        String nowString = now.toString();
        CsvMapWriter successWriter = new CsvMapWriter(new FileWriter(new File(alanLoaderResult, "Success" + nowString + ".csv")),
                CsvPreference.EXCEL_PREFERENCE);
        CsvMapWriter failureWriter = new CsvMapWriter(new FileWriter(new File(alanLoaderResult, "Failure"+ nowString + ".csv")),
                CsvPreference.EXCEL_PREFERENCE);

        //migrator.insertAttachments(reverseAttachmentIdMap, attachmentIdMap, successWriter, failureWriter);

    }

    private void extractAttachments(Map<String, String> idMapper,
                                    Map<String, String> ownerIdMap) throws IOException {//config.setTraceMessage(true);

        try {
            ConnectorConfig srcConfig = new ConnectorConfig();
            srcConfig.setAuthEndpoint(ENDPOINT);
            srcConfig.setUsername(SRC_USERNAME);
            srcConfig.setPassword(SRC_PASSWORD);

            EnterpriseConnection src_connection = Connector.newConnection(srcConfig);

            queryAttachments(idMapper, ownerIdMap, src_connection);


        } catch (ConnectionException e1) {
            e1.printStackTrace();
        }
    }


    private void insertAttachments(Map<String, String> reverseMap,
                                   Map<String, String> idMap,
                                   CsvMapWriter success,
                                   CsvMapWriter failure) throws IOException {
        CsvMapReader reader = null;
        try {
            ConnectorConfig targetConfig = new ConnectorConfig();
            targetConfig.setUsername(TAGET_USERNAME);
            targetConfig.setPassword(TARGET_PASSWORD);

            EnterpriseConnection target_connection = Connector.newConnection(targetConfig);

            CsvReadingInfo info = getIdMapperReader(ATTACHMENT_EXTRACT);

            Attachment[] attachmentDetails;
            int batchesCompleted = 0;
            success.writeHeader(SUCCESS_HEADER);
            failure.writeHeader(FAILURE_HEADER);
            while ((attachmentDetails = getAttachmentDetails(info, idMap)).length > 0) {
                String result[] = insertIntoTarget(attachmentDetails,
                        target_connection,
                        reverseMap,
                        success,
                        failure);
                for (int i = 0; i < result.length; i++)
                    System.out.println(i + " : " + result[i]);
                System.out.println("*******************");
                System.out.println((((batchesCompleted) * BATCH_SIZE) + result.length) + " records inserted");

                ++batchesCompleted;
                System.out.println("*******************");
            }

        } catch (ConnectionException e1) {
            e1.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                success.close();
                failure.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }


    private static Attachment[] getAttachmentDetails(CsvReadingInfo info,
                                                     Map<String, String> idMap) throws IOException {
        System.out.println("ENTERING ATTACHMENT READER");

        Map<String, String> currLine;
        int i = 0;
        ArrayList<Attachment> results = new ArrayList<Attachment>();

        while (i < BATCH_SIZE && (currLine = info.getReader().read(info.getNameMapping())) != null) {
            Attachment att = new Attachment();

            String legacyParentId = currLine.get("ParentId");
            String newParentId = idMap.get(legacyParentId);
            att.setParentId(newParentId);
            att.setName(currLine.get("Name"));
            att.setContentType(currLine.get("ContentType"));
            //att.setIsDeleted(Boolean.valueOf(currLine.get("isDeleted")));
            att.setIsPrivate(Boolean.valueOf(currLine.get("isPrivate")));
            if (currLine.get("owner") != null && currLine.get("owner").length() > 0)
                att.setOwnerId(currLine.get("owner"));

            System.out.println("Parent:" + newParentId);
            System.out.println("Name:" + att.getName());
            System.out.println("contentType:" + att.getContentType());
            //System.out.println("isDeleted:" + att.getIsDeleted());
            System.out.println("isPrivate:" + att.getIsPrivate());
            System.out.println("\n\n");

            att.setBody(readFromDisk("./" + TARGET_DIR + "/" + legacyParentId, att.getName()));

            results.add(att);
            i++;
        }
        Attachment[] returnResult = new Attachment[results.size()];
        return results.toArray(returnResult);
    }


    private static String[] insertIntoTarget(Attachment[] atts,
                                             EnterpriseConnection connection,
                                             Map<String, String> reverseMap,
                                             CsvMapWriter success,
                                             CsvMapWriter failure) {
        String[] result = new String[atts.length];


        try {

            for (int i=0; i<atts.length; i++) {
                atts[i].setBody(readFromDisk(reverseMap.get(atts[i].getParentId()), atts[i].getName()));
            }

            SaveResult[] saveResults = connection.create(atts);
            for (int i = 0; i < saveResults.length; i++) {
                if (saveResults[i].isSuccess()) {
                    System.out.println("Successfully created Account ID: " + saveResults[i].getId());
                    result[i] = saveResults[i].getId();
                    final HashMap<String, ? super Object> data = new HashMap<String, Object>();
                    int j=0;
                    data.put(SUCCESS_HEADER[j++], result[i] == null? "": result[i]);
                    data.put(SUCCESS_HEADER[j++], atts[i].getParentId() == null ? "" : atts[i].getParentId());
                    data.put(SUCCESS_HEADER[j++], atts[i].getName() == null ? "" : atts[i].getName());
                    data.put(SUCCESS_HEADER[j++], atts[i].getContentType() == null ? "" : atts[i].getContentType());
                    data.put(SUCCESS_HEADER[j++], atts[i].getIsPrivate() == null ? "" : atts[i].getIsPrivate());
                    data.put(SUCCESS_HEADER[j++], atts[i].getOwnerId() == null ? "" : atts[i].getOwnerId());
                    try {
                        if (success == null) {
                            System.out.println("success file is null");
                        }
                        success.write(data, SUCCESS_HEADER);
                    } catch (IOException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }


                } else {
                    final HashMap<String, ? super Object> data = new HashMap<String, Object>();
                    int j=0;
                    data.put(FAILURE_HEADER[j++], saveResults[i].getId() == null ? "" : saveResults[i].getId());
                    data.put(FAILURE_HEADER[j++], atts[i].getParentId() == null ? "" : atts[i].getParentId());
                    data.put(FAILURE_HEADER[j++], atts[i].getName() == null ? "" : atts[i].getName());
                    data.put(FAILURE_HEADER[j++], atts[i].getContentType() == null ? "" : atts[i].getContentType());
                    data.put(FAILURE_HEADER[j++], atts[i].getIsPrivate() == null ? "" : atts[i].getIsPrivate());
                    data.put(FAILURE_HEADER[j++], atts[i].getOwnerId() == null ? "" : atts[i].getOwnerId());
                    data.put(FAILURE_HEADER[j++], saveResults[i].getErrors()[0].getMessage());
                    try {
                        failure.write(data, FAILURE_HEADER);
                    } catch (IOException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }

                    System.out.println("Error: could not create Account " + "record number " + i + ".");
                    System.out.println("   The error reported was: " +
                            saveResults[i].getErrors()[0].getMessage() + "\n");
                    System.out.println("Error Account ID: " + saveResults[i].getId());
                    result[i] = saveResults[i].getId();
                }
            }
        } catch (ConnectionException ce) {
            ce.printStackTrace();
        }
        return result;
    }

    private Map<String, String> getLegacyToNewIdMap(String fileName) throws IOException {

        CsvReadingInfo readingInfo = getIdMapperReader(fileName);
        Map<String, String> result = new HashMap<String, String>();

        String[] nameMapping = readingInfo.getNameMapping();
        CsvMapReader reader = readingInfo.getReader();

        Map<String, String> currLine;
        while ((currLine = reader.read(nameMapping)) != null) {
            result.put(currLine.get("LEGACYID"), currLine.get("NEWID"));
            System.out.println(currLine.get("LEGACYID") + " / " + currLine.get("NEWID"));
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


    private static void queryAttachments(Map<String, String> idMap,
                                         Map<String, String> ownerIdMap,
                                         EnterpriseConnection connection) throws ConnectionException {
        FileWriter fw = null;
        CsvMapWriter writer = null;
        try {
            fw = new FileWriter(ATTACHMENT_EXTRACT);
            writer = new CsvMapWriter(fw, CsvPreference.EXCEL_PREFERENCE);

            ArrayList<String> attachmentIds = new ArrayList<String>();


            int pos = 0;


            String[] parentIdArray = idMap.keySet().toArray(new String[0]);

            System.out.println("parentIdArray.length is " + parentIdArray.length);

            while (pos < parentIdArray.length) {
                StringBuffer sb = new StringBuffer(10000);

                System.out.println("pos is " + pos);
                int counter = 0;
                while (pos < parentIdArray.length && counter < BIG_IN_SIZE) {
                    sb.append("'" + parentIdArray[pos] + "'");
                    pos++;
                    counter++;
                    if (pos < parentIdArray.length && counter < BIG_IN_SIZE) {
                        sb.append(",");
                    }
                }
                //System.out.println((parentIdCounter++) + ": Id to be retrieved is " + legacyId);
                QueryResult queryResults = null;
                while (queryResults == null) {
                    try {
                        queryResults = connection.query("Select Id, ParentId " +
                                "From Attachment WHERE parentid in (" +
                                sb.toString() + ")");
                    } catch (Exception e) {
                        e.printStackTrace();
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
                if (queryResults.getSize() > 0) {
                    for (int i = 0; i < queryResults.getRecords().length; i++) {
                        // cast the SObject to a strongly-typed Contact
                        Attachment a = (Attachment) queryResults.getRecords()[i];
                        attachmentIds.add(a.getId());
                    }
                }
            }



            writer.writeHeader(ATTACHMENT_HEADER);

            for (String s : attachmentIds) {

                QueryResult queryResults = connection.query("Select Id, ParentId, Name, ContentType, Body," +
                        "isPrivate, ownerId " +
                        "From Attachment WHERE id = '" + s + "'");
                if (queryResults.getSize() > 0) {
                    System.out.println("record '" + s + "' has attach count = " + queryResults.getSize());
                    System.out.println("queryResults.getRecords().length is " + queryResults.getRecords().length);


                    for (int i = 0; i < queryResults.getRecords().length; i++) {
                        Map<String, ? super Object> currRecord = new HashMap<String, Object>();
                        // cast the SObject to a strongly-typed Contact
                        Attachment a = (Attachment) queryResults.getRecords()[i];

                        int j=0;


                        currRecord.put(ATTACHMENT_HEADER[j++], "" + a.getParentId());
                        currRecord.put(ATTACHMENT_HEADER[j++], "" +a.getName());
                        currRecord.put(ATTACHMENT_HEADER[j++], "" + a.getContentType());
                        currRecord.put(ATTACHMENT_HEADER[j++], "" + a.getIsPrivate());
                        currRecord.put(ATTACHMENT_HEADER[j++], ownerIdMap == null
                                ? ""
                                : ownerIdMap.get(a.getOwnerId()) == null
                                ? ""
                                : ownerIdMap.get(a.getOwnerId()));

                        System.out.println("original parent:" + a.getParentId());
                        System.out.println("Name:" + a.getName());
                        System.out.println("contentType:" + a.getContentType());
                        //System.out.println("isDeleted:" + a.getIsDeleted());
                        System.out.println("isPrivate:" + a.getIsPrivate());


                        //System.out.println("OwnerId:"+ ownerIdMap.get(a.getOwnerId()));

                        writer.write(currRecord, ATTACHMENT_HEADER);

                        writeOnDisk(a.getParentId(), a.getName(), a.getBody());
                        System.out.println("Id: " + a.getId() + " - Name: " +
                                a.getName() + " - parent: " + a.getParentId());

                    }
                }
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

    private static void writeOnDisk(String dir, String fileName, byte[] bdy) {
        try {
            File directory = new File("./" + TARGET_DIR + "/" + dir);
            directory.mkdir();
            String filePath = "./" + TARGET_DIR + "/" + dir + "/" + fileName;
            FileOutputStream fos = new FileOutputStream(filePath);//File OutPutStream is used to write Binary Contents like pictures
            fos.write(bdy);
            fos.close();
            System.out.println(new File(filePath).getAbsolutePath());
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private static byte[] readFromDisk(String dir, String fileName) {
        try {

            File directory = new File("./" + dir);
            //File directory = new File("./" + TARGET_DIR + "/" + dir);
            System.out.println("directory is " + directory.getAbsolutePath());


            File theFile = new File(directory, fileName);
            //File theFile = new File(fileName);
            String filePath =  dir + "/" + fileName;

            System.out.println("parent = " + theFile.getParent());
            System.out.println("path = " + theFile.getAbsolutePath());
            if (theFile.exists()) {
                System.out.println("hi ");
            } else {
                File testFile = new File("./" + TARGET_DIR + "/" + dir);
                if (testFile.exists()) {
                    System.out.println("testfile exists of dir: " + testFile.getAbsolutePath() );
                    theFile = new File(testFile, fileName);
                    if (theFile.exists()) {
                        System.out.println("hi ");
                    } else {
                        System.out.println("shit");
                    }
                } else {
                    System.out.println("testfile does NOT exist for dir: " + testFile.getAbsolutePath());

                    System.out.println("shit");
                }
            }
            FileInputStream fis = new FileInputStream(theFile);//File OutPutStream is used to write Binary Contents like pictures
            byte[] bdy = new byte[fis.available()];
            fis.read(bdy);
            fis.close();
            System.out.println(new File(filePath).getAbsolutePath());
            return bdy;
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
            return null;
        }
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
